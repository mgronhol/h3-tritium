#!/usr/bin/env python

#
#   Copyright 2013 Markus Gronholm <markus@alshain.fi> / Alshain Oy
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.


class RedisProtocol(object):
	def __init__( self, conn ):
		self.conn = conn
		self.buf = ""

	def _pack_list( self, data ):	
		#print "packing list", data
		out = "*%i\r\n" % len( data )
		for entry in data:
			if isinstance( entry, int ):
				out += ":%i\r\n" % entry
			elif isinstance( entry, list ):
				out += self._pack_list( entry )
			elif isinstance( entry, dict ):
				out += self._pack_dict( entry )
			else:
				out += "$%i\r\n" % len( str(entry) ) 
				out += "%s\r\n" % str( entry )
		return out
		
	def _pack_dict( self, data ):
		#print "packing dict", data
		out = "*%i\r\n" % (len( data ) * 2)
		for (key, value) in data.items():
			out += "$%i\r\n" % len( key )
			out += "%s\r\n" % key
			if isinstance( value, int ):
				out += ":%i\r\n" % value
			elif isinstance( value, list ):
				out += self._pack_list( value )
			elif isinstance( value, dict ):
				out += self._pack_dict( value )
			else:
				out += "$%i\r\n" % len( value )
				out += "%s\r\n" % value
		return out

	def send_response( self, message ):
		if isinstance( message, str ):
			self.conn.send( "+%s\r\n" % repr(message)[1:-1])
		elif isinstance( message, list ):
			self.conn.send( self._pack_list( message ) )
		elif isinstance( message, dict ):
			self.conn.send( self._pack_dict( message ) )
		elif isinstance( message, int ):
			self.conn.send(":%i\r\n" % message )
	
	def send_error( self, message ):
		self.conn.send( '-%s\r\n' % repr(message)[1:-1] )



	def recv_bulk( self ):
		#print "bulk"
		while '\r\n' not in self.buf:
			self._recv()
		
		if not self.buf.startswith( "$" ):
			return False
		
		(line, rest) = self.buf.split( "\r\n", 1 )
		next_bytes = int( line[1:] )
		self.buf = rest
		while len( self.buf ) < next_bytes + 2:
			self._recv()
		
		(line, rest) = self.buf.split( "\r\n", 1 )
		
		self.buf = rest
		
		return line
	
	def recv_string( self ):
		#print "string"
		while '\r\n' not in self.buf:
			self._recv()
		
		if not self.buf.startswith( "+" ):
			return False
		
		(line, rest) = self.buf.split( "\r\n", 1 )
		self.buf = rest
		
		return line[1:]
	
	def recv_error( self ):
		while '\r\n' not in self.buf:
			self._recv()
		
		if not self.buf.startswith( "-" ):
			return False
		
		(line, rest) = self.buf.split( "\r\n", 1 )
		self.buf = rest
		
		return line[1:]
	
	
	def recv_integer( self ):
		#print "integer"
		while '\r\n' not in self.buf:
			self._recv()
		
		if not self.buf.startswith( ":" ):
			return False
		
		(line, rest) = self.buf.split( "\r\n", 1 )
		self.buf = rest
		
		return int(line[1:])
			
	def recv_multibulk( self ):
		#print "multibulk"
		while '\r\n' not in self.buf:
			self._recv()
		
		if not self.buf.startswith( "*" ):
			return False
		
		(line, rest) = self.buf.split( "\r\n", 1 )
		self.buf = rest
		N = int( line[1:] )
		#print "Nparams", N
		out = []
		for i in range( N ):
			while len( self.buf ) < 1:
				self._recv()
			#print "multibulk selector", self.buf[0]
			if self.buf.startswith( "+" ):
				out.append( self.recv_string() )
			elif self.buf.startswith( "-" ):
				out.append( self.recv_error() )
			elif self.buf.startswith( ":" ):
				out.append( self.recv_integer() )
			elif self.buf.startswith( "$" ):
				out.append( self.recv_bulk() )
			else:
				out.append( self.recv_multibulk() )
		return out
				
	
	def _recv( self ):
		
		self.buf += self.conn.recv(1024)
		print self.buf
	
	def receive(self):
		while len( self.buf ) < 1:
			self._recv()
		
		if self.buf.startswith( "+" ):
			return self.recv_string()

		elif self.buf.startswith( "-" ):
			return self.recv_error()
		
		elif self.buf.startswith( ":" ):
			return self.recv_integer()

		elif self.buf.startswith( "$" ):
			return self.recv_bulk()

		elif self.buf.startswith( "*" ):
			return self.recv_multibulk()
		


