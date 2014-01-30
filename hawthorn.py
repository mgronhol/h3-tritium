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



import sys
import gevent
from gevent.server import StreamServer
import gevent.socket

import collections

import json

import libs.RedisProtocol as RedisProtocol
from libs.Hawthorn import (Edge, Node, Graph, QueryEngine)
import libs.Storage as Storage
import libs.HawthornProtocol as HawthornProtocol


class ReplicatedStorage( Storage.HawthornStorage ):
	def __init__( self, addrs ):
	
		self.addrs = addrs
		self.conns = []
		for addr in self.addrs:
			(host, port) = addr.split(":")
			port = int(port)
			self.conns.append( HawthornProtocol.HawthornClient( host, port ) )
		self._suppress = False
	
	def suppress( self, value ):
		self._suppress = value
	
	def load( self, db ):
		pass
	
	def save( self, op, params ):	
		if self._suppress:
			return
		
		if op == "CREATE":
			node_id = params[0]
			for conn in self.conns:
				conn.create( node_id )
		
		elif op == "DELETE":
			node_id = params[0]
			for conn in self.conns:
				conn.delete( node_id )
		
		elif op == "CONNECT":
			source = params[0]
			target = params[1]
			edge_type = params[2]
			weight = params[3]
			for conn in self.conns:
				conn.connect( source, target, edge_type, weight )
		
		elif op == "DISCONNECT":
			source = params[0]
			target = params[1]
			edge_type = params[2]
			for conn in self.conns:
				conn.disconnect( source, target, edge_type )
		
		elif op == "SET":
			node_id = params[0]
			key = params[1]
			value = params[2]
			for conn in self.conns:
				conn.set( node_id, key, value )
		
		elif op == "UNSET":
			node_id = params[0]
			key = params[1]
			for conn in self.conns:
				conn.unset( node_id, key )
		
		
	

class MultiStorage( Storage.HawthornStorage ):
	def __init__( self, storages ):
		self.storages = storages
	
	def suppress( self, value ):
		for store in self.storages:
			store.suppress( value )
	
	def load( self, db ):
		for store in self.storages:
			store.load( db )
	
	def save( self, op, params ):
		for store in self.storages:
			store.save( op, params )















def parse_int( value ):
	
	if isinstance( value, int ):
		return value
	
	try:
		if value.startswith( "0x" ):
			node_id = int( value, 16 )
		else:
			node_id = int( value )
	except:
		return False
	return node_id



	

class Hawthorn( object ):
	def __init__( self, storage, config ):
		self.graphs = {}
		for i in range( 16 ):
			self.graphs[i] = Graph()
		
		self.queries = {}
		self.next_query_id = 1
		
		self.storage = storage
		
		self.storage.suppress( True )
		self.storage.load( self )
		self.storage.suppress( False )
		
		self.config = config
		
	
	def start_query( self, db_id ):
		qid = self.next_query_id
		self.queries[ qid ] = QueryEngine( self.graphs[ db_id ] )
		
		self.next_query_id += 1
		return qid
	
	def end_query( self, qid ):
		if qid in self.queries:
			del self.queries[qid]
			
		
	def execute( self, qid, command ):
		if qid not in self.queries:
			return (False, "Invalid query id.")
		
		query = self.queries[qid]
		
		op = command[0]
		params = command[1:]
		
		print op, params
		
		if op in ["CREATE", "DELETE"]:
			if len( params ) != 1:
				return (False, "Invalid parameter count (%i), should be %i." % ( len(params), 1 ) )
			if op == 'CREATE':
				node_id = parse_int( params[0] )
				
				if not node_id:
					return (False, "Invalid node id (%s)." % params[0] )
				
				query.graph.create( node_id )
				self.storage.save( op, params )
				return (True, "OK")
				
				
			elif op == 'DELETE':
				node_id = parse_int( params[0] )
				
				if not node_id:
					return (False, "Invalid node id (%s)." % params[0] )
				
				self.storage.save( op, params )
				return query.graph.remove_node( node_id )
		
		if op in ["SET", "UNSET", "CONNECT", "DISCONNECT"]:
		
			if op == 'SET':
				if len( params ) != 3:
					return (False, "Invalid parameter count (%i), should be %i." % ( len(params), 2 ) )
				
				node_id = parse_int( params[0] )
				
				if not node_id:
					return (False, "Invalid node id (%s)." % params[0] )
				
				key = params[1]
				value = params[2]
				
				self.storage.save( op, params )
				return query.graph.set_property( node_id, key, value )
				
				
			elif op == 'UNSET':
				if len( params ) != 2:
					return (False, "Invalid parameter count (%i), should be %i." % ( len(params), 2 ) )
				
				node_id = parse_int( params[0] )
				
				if not node_id:
					return (False, "Invalid node id (%s)." % params[0] )
				
				self.storage.save( op, params )
				return query.graph.remove_property( node_id, params[1] )

			elif op == 'CONNECT':
				if len( params ) != 4:
					return (False, "Invalid parameter count (%i), should be %i." % ( len(params), 4 ) )

				source = parse_int( params[0] )
				target = parse_int( params[1] )
				
				edge_type = params[2]
				value = params[3]
				
				if not source:
					return (False, "Invalid source id (%s)." % params[0] )
				
				if not target:
					return (False, "Invalid target id (%s)." % params[1] )
				
				self.storage.save( op, params )
				return query.graph.connect( source, target, edge_type, value )
			
				
			elif op == 'DISCONNECT':
				if len( params ) != 3:
					return (False, "Invalid parameter count (%i), should be %i." % ( len(params), 3 ) )
				
				
				source = parse_int( params[0] )
				target = parse_int( params[1] )
				
				edge_type = params[2]
				
				if not source:
					return (False, "Invalid source id (%s)." % params[0] )
				
				if not target:
					return (False, "Invalid target id (%s)." % params[1] )
				
				self.storage.save( op, params )
				return query.graph.disconnect( source, target, edge_type )
			
		
		elif op in ["GET", "FETCH", "EDGES", "CLEAR", "CLEAR-ALL"]:
			
			if op == 'GET':
				if len( params ) != 1:
					return (False, "Invalid parameter count (%i), should be %i." % ( len(params), 1 ) )
				
				node_id = parse_int( params[0] )
				
				if not node_id:
					return (False, "Invalid node id (%s)." % params[0] )
				
				return query.graph.get_node( node_id )
				
			elif op == 'FETCH':
				if len( params ) != 1:
					return (False, "Invalid parameter count (%i), should be %i." % ( len(params), 1 ) )
				
				qset = params[0]
				
				return query.fetch( qset )

			elif op == 'EDGES':
				if len( params ) != 1:
					return (False, "Invalid parameter count (%i), should be %i." % ( len(params), 1 ) )
				
				node_id = parse_int( params[0] )
				
				if not node_id:
					return (False, "Invalid node id (%s)." % params[0] )
				
				#return query.graph.get_node( node_id )
				
				status, fedges = query.graph.get_forward_edges( node_id )
				if not status:
					return (status, fedges)
				
				status, bedges = query.graph.get_backward_edges( node_id )
				if not status:
					return (status, bedges)
				
				return (True, {"forward": fedges, "backward": bedges} )
			
			elif op == 'CLEAR':
				if len( params ) != 1:
					return (False, "Invalid parameter count (%i), should be %i." % ( len(params), 1 ) )
				
				qset = params[0]
				
				return query.clear( qset )
				
		
		elif op in ["START", "FIND", "FORWARD", "BACKWARD", "FILTER", "APPEND", "UNION", "INTERSECT", "DIFFERENCE"]:
			
			if op == 'START':
				if len( params ) < 2:
					return (False, "Invalid parameter count (%i), should be > %i." % ( len(params), 1 ) )
				
				target = params[0]
				
				for param in params[1:]:
					node_id = parse_int( param )
					if not node_id:
						return (False, "Invalid node id (%s)." % param )
					
					#query.start( target, node_id )
				
				#return (True, len( params ) )
				return query.start( target, node_id )
				
			elif op == 'FIND':

				if len( params ) != 4:
					return (False, "Invalid parameter count (%i), should be %i." % ( len(params), 4 ) )
				
				target = params[0]
				key = params[1]
				value = params[2]
				operator = params[3]
				
				return query.find( key, value, operator, target )
	
			elif op == 'FORWARD':
				if len( params ) < 3:
					return (False, "Invalid parameter count (%i), should be > %i." % ( len(params), 2 ) )
				
				target = params[0]
				source = params[1]
				types = params[2:]
				
				
				return query.forward( source, target, types )
			
			elif op == 'BACKWARD':
				if len( params ) < 3:
					return (False, "Invalid parameter count (%i), should be > %i." % ( len(params), 2 ) )
				
				target = params[0]
				source = params[1]
				types = params[2:]
				
				
				return query.backward( source, target, types )
			
			elif op == 'FILTER':
				if len( params ) != 5:
					return (False, "Invalid parameter count (%i), should be %i." % ( len(params), 5 ) )
				
				target = params[0]
				source = params[1]
				key = params[2]
				value = params[3]
				operator = params[4]
				
				return query.filter( source, target, key, value, operator )
			
			elif op == 'APPEND':
				if len( params ) != 3:
					return (False, "Invalid parameter count (%i), should be %i." % ( len(params), 3 ) )
				
				target = params[0]
				source0 = params[1]
				source1 = params[2]
				
				return query.append( source0, source1, target )
			
			elif op == 'UNION':
				if len( params ) != 3:
					return (False, "Invalid parameter count (%i), should be %i." % ( len(params), 3 ) )
				
				target = params[0]
				source0 = params[1]
				source1 = params[2]
				
				return query.union( source0, source1, target )

			elif op == 'INTERSECTION':
				if len( params ) != 3:
					return (False, "Invalid parameter count (%i), should be %i." % ( len(params), 3 ) )
				
				target = params[0]
				source0 = params[1]
				source1 = params[2]
				
				return query.intersection( source0, source1, target )

			elif op == 'DIFFERENCE':
				if len( params ) != 3:
					return (False, "Invalid parameter count (%i), should be %i." % ( len(params), 3 ) )
				
				target = params[0]
				source0 = params[1]
				source1 = params[2]
				
				return query.difference( source0, source1, target )
			
		return (False, "Unknown command '%s'." % op )

	def get_handler( self ):
		def handler( socket, address ):
			qid = self.start_query( 0 )
			#print "Connection accepted"
			conn = RedisProtocol.RedisProtocol( socket )
			try:
				while True:
					data = conn.receive()
					if not data:
						break
					
					(status, response) = self.execute( qid, data )
					
					if status:
						conn.send_response( response )
					else:
						conn.send_error( response )
					
					#print status, response
					
					
					
			except gevent.socket.error:
				#print sys.exc_info()
				#print "Connection closed"
				pass
				
			
			self.end_query( qid )
			socket.close()
		
		return handler


	def run( self ):
		#server = StreamServer( ('127.0.0.1', 7778), self.get_handler() )
		print "Starting H3 Tritium @ %s:%i.." % ( self.config["host"], self.config["port"] )
		server = StreamServer( (self.config["host"], self.config["port"]), self.get_handler() )
		try:
			server.serve_forever()
		except KeyboardInterrupt:
			pass
		
		#server.kill()



config = {}

with open( sys.argv[1], 'r') as handle:
	config = json.load( handle )




appendlog = Storage.AppendLogStorage( config["database"] )

replicator = ReplicatedStorage( config["replication"]["hosts"] )


multistore = MultiStorage([ appendlog, replicator ])


hawt = Hawthorn( multistore, config )
hawt.run()
