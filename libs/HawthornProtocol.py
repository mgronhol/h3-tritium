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

import RedisProtocol

import socket

def _encode_as_dict( entries ):
	out = {}
	#print "entries", entries
	for i in range( 0, len( entries ), 2 ):
		out[ entries[i] ] = entries[i+1]
	return out	

def _encode_as_two_deep_dict( entries ):
	out = {}
	for i in range( 0, len( entries ), 2 ):
		if isinstance( entries[i+1], list ):
			#tmp = [ _encode_as_dict( x ) for x in entries[i+1] ]
			tmp = []
			if len(entries[i+1]) > 0:
				if isinstance( entries[i+1][0], list ):
					tmp = [ _encode_as_dict( x ) for x in entries[i+1] ]
				else:
					tmp = _encode_as_dict( entries[i+1] )
					
		else:
			tmp = entries[i+1]
		out[ entries[i] ] = tmp
	return out	


class HawthornClient( object ):
	def __init__( self, host, port ):
		self.host = host
		self.port = port
		
		self.conn = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
		self.conn.setsockopt( socket.SOL_SOCKET, socket.SO_REUSEADDR, 1 )
		
		self.conn.connect( (self.host, self.port) )
		self.redis = RedisProtocol.RedisProtocol( self.conn )
	
		self._error = ""
	
	
	def close( self ):
		self.conn.close()
	
	def _parse_result( self, response, encoder = None ):
		#print "DEBUG", response
		if isinstance( response, str ) and response.startswith( "-" ):
			self._error = response[1:]
			return False
		else:
			if encoder:
				return encoder( response )
			else:
				return response
	
	
	def get( self, node_id ):
		self.redis.send_response( ["GET", node_id] )
		return self._parse_result( self.redis.receive(), _encode_as_two_deep_dict )

	def edges( self, node_id ):
		self.redis.send_response( ["EDGES", node_id] )
		return self._parse_result( self.redis.receive(), _encode_as_two_deep_dict )

		
	def set( self, node_id, key, value ):
		self.redis.send_response( ["SET", node_id, key, value] )
		return self._parse_result( self.redis.receive() )

	def unset( self, node_id, key ):
		self.redis.send_response( ["UNSET", node_id, key] )
		return self._parse_result( self.redis.receive() )
		
	def create( self, node_id ):
		self.redis.send_response( ["CREATE", node_id] )
		return self._parse_result( self.redis.receive() )

	def delete( self, node_id ):
		self.redis.send_response( ["DELETE", node_id] )
		return self._parse_result( self.redis.receive() )
		
	def fetch( self, queryset ):
		self.redis.send_response( ["FETCH", queryset] )
		return self._parse_result( self.redis.receive() )
	
	def clear( self, queryset ):
		self.redis.send_response( ["CLEAR", queryset] )
		return self._parse_result( self.redis.receive() )
	
	def connect( self, source, target, edge_type, weight ):
		self.redis.send_response( ["CONNECT", source, target, edge_type, weight] )
		return self._parse_result( self.redis.receive(), _encode_as_dict )
	
	def disconnect( self, source, target, edge_type ):
		self.redis.send_response( ["DISCONNECT", source, target, edge_type] )
		return self._parse_result( self.redis.receive() )

	def start( self, queryset, nodes ):
		self.redis.send_response( ["START", queryset] + nodes )
		return self._parse_result( self.redis.receive() )
	
	def find( self, resultset, key, value, operator ):
		self.redis.send_response( ["FIND", resultset, key, value, operator] )
		return self._parse_result( self.redis.receive() )
		
	def forward( self, target, source, types ):
		self.redis.send_response( ["FORWARD", target, source] + types )
		return self._parse_result( self.redis.receive() )

	def backward( self, target, source, types ):
		self.redis.send_response( ["BACKWARD", target, source] + types )
		return self._parse_result( self.redis.receive() )

	def filter( self, target, source, key, value, operator ):
		self.redis.send_response( ["FILTER", target, source, key, value, operator] )
		return self._parse_result( self.redis.receive() )

	def append( self, target, source0, source1 ):
		self.redis.send_response( ["APPEND", target, source0, source1] )
		return self._parse_result( self.redis.receive() )

	def union( self, target, source0, source1 ):
		self.redis.send_response( ["UNION", target, source0, source1] )
		return self._parse_result( self.redis.receive() )

	def intersection( self, target, source0, source1 ):
		self.redis.send_response( ["INTERSECTION", target, source0, source1] )
		return self._parse_result( self.redis.receive() )

	def difference( self, target, source0, source1 ):
		self.redis.send_response( ["DIFFERENCE", target, source0, source1] )
		return self._parse_result( self.redis.receive() )


