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

import RedisProtocol

def parse_int( value ):
	try:
		if value.startswith( "0x" ):
			node_id = int( value, 16 )
		else:
			node_id = int( value )
	except:
		return False
	return node_id



class Edge:
	SOURCE = 0
	TARGET = 1
	TYPE = 2
	WEIGHT = 3

	@staticmethod
	def create( source, target, edge_type, weight ):
		return [source, target, edge_type, weight]


class Node:
	ID = 0
	PROPERTIES = 1
	FORWARD_EDGES = 2
	BACKWARD_EDGES = 3
	
	@staticmethod
	def create( id, props, forward, backward ):
		return [id, props, forward, backward]
	
	@staticmethod
	def get_property( node, key ):
		props = node[ Node.PROPERTIES ]
		for entry in props:
			if entry[0] == key:
				return entry[1]
		return None
	
	@staticmethod
	def set_property( node, key, value ):
		props = node[ Node.PROPERTIES ]
		for i in range( len( props ) ):
			if props[i][0] == key:
				props[i] = (props[i][0], value)
				return False
		
		props.append( (key, value) )
		return True
	
	@staticmethod
	def remove_property( node, key ):
		props = node[ Node.PROPERTIES ]
		for i in range in props:
			if props[i][0] == key:
				del props[i]
				return None
		return None
	
	
	@staticmethod
	def add_forward_edge( node, edge ):
		edges = node[ Node.FORWARD_EDGES ]
		for i in range( len( edges ) ):
			if edges[i][0] == edge[0]:
				if edges[i][1] == edge[1]:
					if edges[i][2] == edge[2]:
						edges[i] = edge
						return False
		
		edges.append( edge )
		return True
		
	@staticmethod
	def add_backward_edge( node, edge ):
		edges = node[ Node.BACKWARD_EDGES]
		for i in range( len( edges ) ):
			if edges[i][0] == edge[0]:
				if edges[i][1] == edge[1]:
					if edges[i][2] == edge[2]:
						edges[i] = edge
						return False
		
		edges.append( edge )
		return True
			
	@staticmethod
	def remove_forward_edge( node, edge ):
		edges = node[ Node.FORWARD_EDGES ]
		for i in range( len( edges ) ):
			if edges[i][0] == edge[0]:
				if edges[i][1] == edge[1]:
					if edges[i][2] == edge[2]:
						del edges[i]
						return True
		return False
		

	@staticmethod
	def remove_backward_edge( node, edge ):
		edges = node[ Node.BACKWARD_EDGES ]
		for i in range( len( edges ) ):
			if edges[i][0] == edge[0]:
				if edges[i][1] == edge[1]:
					if edges[i][2] == edge[2]:
						del edges[i]
						return True
		return False
		
	@staticmethod
	def get_forward( node ):
		edges = node[ Node.FORWARD_EDGES ]
		return edges

	@staticmethod
	def get_backward( node ):
		edges = node[ Node.BACKWARD_EDGES ]
		return edges


class Graph(object):
	def __init__(self):
		self.nodes = {}
		self.types = {}
		self.props = {}
		
		self.reverse_types = {}
		self.reverse_props = {}
		
		
		self.next_type_id = 1
		self.next_prop_id = 1
		
		
	
	def create( self, id ):
		self.nodes[id] = Node.create( id, [], [], [] )
	
	def connect( self, source, target, edge_type, value ):
		type_id = self.next_type_id
		if edge_type in self.types:
			type_id = self.types[ edge_type ]
		else:
			self.types[ edge_type ] = self.next_type_id
			self.reverse_types[ self.next_type_id ] = edge_type
			self.next_type_id += 1
		
		if source not in self.nodes:
			return (False, "Source node (%i) not in graph." % source )

		if target not in self.nodes:
			return (False, "Target node (%i) not in graph." % target )
			
		
		edge = Edge.create( source, target, type_id, value )
		
		Node.add_forward_edge( self.nodes[ source ], edge )
		Node.add_backward_edge( self.nodes[ target ], edge )
		
		
		return (True, {"source": edge[ Edge.SOURCE ], "target": edge[ Edge.TARGET ], "type": edge_type, "weight": edge[ Edge.WEIGHT ] })
		
	def disconnect( self, source, target, edge_type ):
		if source not in self.nodes:
			return (False, "Source node (%i) not in graph." % source )

		if target not in self.nodes:
			return (False, "Target node (%i) not in graph." % target )
		
		if edge_type not in self.types:
			return (False, "Edge type (%s) not defined." % edge_type )
		
		edge_id = self.types[ edge_type ]
		
		edge = Edge.create( source, target, edge_id, 0 )
		
		Node.remove_forward_edge( self.nodes[ source ], edge )
		Node.remove_backward_edge( self.nodes[ target ], edge )
		
		return (True, {"source": edge[ Edge.SOURCE ], "target": edge[ Edge.TARGET ], "type": edge_type, "weight": edge[ Edge.WEIGHT ] })
	
	def remove_node( self, node_id ):
		
		if node_id not in self.nodes:
			return (False, "Node (%i) not in graph." % node_id )
		
		forwards = self.nodes[node_id][ Node.FORWARD_EDGES ]
		backwards = self.nodes[node_id][ Node.BACKWARD_EDGES ]
		
		for edge in forwards:
			self.disconnect( edge[Edge.SOURCE], edge[Edge.TARGET], self.reverse_types[ edge[Edge.TYPE] ] )
			
		for edge in backwards:
			self.disconnect( edge[Edge.SOURCE], edge[Edge.TARGET], self.reverse_types[ edge[Edge.TYPE] ] )
		
		del self.nodes[ node_id ]	
		
		return (True, "OK")
		
	
	def set_property( self, node_id, key, value ):
		if node_id not in self.nodes:
			return (False, "Node (%i) not in graph." % node_id )
		
		key_id = self.next_prop_id 
		if key in self.props:
			key_id = self.props[key]
		else:
			self.props[key] = self.next_prop_id
			self.reverse_props[ self.next_prop_id ] = key
			self.next_prop_id += 1
		
		node = self.nodes[ node_id ]
		Node.set_property( node, key_id, value )
		
		return (True, "OK")
	
	def get_property( self, node_id, key ):
		if node_id not in self.nodes:
			return (False, "Node (%i) not in graph." % node_id )

		key_id = self.props[key]
		node = self.nodes[ node_id ]
		return Node.get_property( node, key_id )
	
	def get_node( self, node_id ):
		if node_id not in self.nodes:
			return (False, "Node (%i) not in graph." % node_id )
		
		node = self.nodes[node_id]
		
		out = {}
		out["id"] = node_id
		
		out["properties"] = {}
		
		for entry in node[ Node.PROPERTIES ]:
			key = self.reverse_props[ entry[0] ]
			value = entry[1]
			
			out["properties"][key] = value
		
		return (True, out)
	
	def get_forward_edges( self, node_id ):
		if node_id not in self.nodes:
			return (False, "Node (%i) not in graph." % node_id )

		edges = self.nodes[node_id][ Node.FORWARD_EDGES ]
		
		out = []
		
		for edge in edges:
			type_value = self.reverse_types[ edge[ Edge.TYPE ] ]
			
			out.append({"source": edge[ Edge.SOURCE ], "target": edge[ Edge.TARGET ], "type": type_value, "weight": edge[ Edge.WEIGHT ] })
		
		return (True, out)
	
	def get_backward_edges( self, node_id ):
		if node_id not in self.nodes:
			return (False, "Node (%i) not in graph." % node_id )

		edges = self.nodes[node_id][ Node.BACKWARD_EDGES ]
		
		out = []
		
		for edge in edges:
			type_value = self.reverse_types[ edge[ Edge.TYPE ] ]
			
			out.append({"source": edge[ Edge.SOURCE ], "target": edge[ Edge.TARGET ], "type": type_value, "weight": edge[ Edge.WEIGHT ] })
		
		return (True, out)
	


class QueryEngine( object ):
	def __init__( self, graph ):
		self.graph = graph
		self.querysets = {}
		
		self.results = []
		
		self.predicates = {
			'=' : lambda v0, v1: v0 == v1,
			'!=': lambda v0, v1: v0 != v1,
			}
	
	def start( self, qset, node_id ):
		nodes = node_id
		if isinstance(node_id, int):
			nodes = [node_id]
		
		self.querysets[ qset ] = nodes
		
		return (True ,len( self.querysets[ qset ] ) )
		
	
	def forward( self, source, target, types ):
		if source not in self.querysets:
			return (False, "Queryset (%s) not found." % source )

		source_nodes = self.querysets[ source ]
		
		result = []
		for node in source_nodes:
			status, edges = self.graph.get_forward_edges( node )
			for edge in edges:
				if edge["type"] in types:
					result.append( edge["target"] )
		
		self.querysets[ target ] = result
		
		return (True, len( result ) )
	
	def backward( self, source, target, types ):
		if source not in self.querysets:
			return (False, "Queryset (%s) not found." % source )

		source_nodes = self.querysets[ source ]
		
		result = []
		for node in source_nodes:
			status, edges = self.graph.get_backward_edges( node )
			for edge in edges:
				if edge["type"] in types:
					result.append( edge["source"] )
		
		self.querysets[ target ] = result
		
		return (True, len( result ) )
	
	
	def filter( self, source, target, key, value, operator ):
		if source not in self.querysets:
			return (False, "Queryset (%s) not found." % source )
		
		if operator not in self.predicates:
			return (False, "Operator (%s) is not defined." % operator )

		
		predicate = self.predicates[ operator ]
		
		source_nodes = self.querysets[ source ]
		result = []
		
		for node_id in source_nodes:
			status, node = self.graph.get_node( node_id )
			if predicate( node[key], value ):
				result.append( node_id )
		
		self.querysets[ target ] = result
		
		return (True, len( result ))

	def unique( self, source, target ):
		
		if source not in self.querysets:
			return (False, "Queryset (%s) not found." % source )

		
		source_nodes = self.querysets[ source ]
		result = []
		for node in source_nodes:
			if node not in result:
				result.append( node )
		
		self.querysets[ target ] = result
		
		return (True, len( result ) )

	def append( self, source0, source1, target ):
		
		if source0 not in self.querysets:
			return (False, "Queryset (%s) not found." % source0 )
		
		if source1 not in self.querysets:
			return (False, "Queryset (%s) not found." % source1 )

		
		
		source_nodes0 = self.querysets[ source0 ]
		source_nodes1 = self.querysets[ source1 ]
		result = []
		
		result.extend( source_nodes0 )
		result.extend( source_nodes1 )
		
		self.querysets[ target ] = result
		
		return (True, len( result ))

	
	def union( self, source0, source1, target ):
		if source0 not in self.querysets:
			return (False, "Queryset (%s) not found." % source0 )
		
		if source1 not in self.querysets:
			return (False, "Queryset (%s) not found." % source1 )

		source_nodes0 = self.querysets[ source0 ]
		source_nodes1 = self.querysets[ source1 ]
		result = []
		
		set0 = set( source_nodes0 )
		set1 = set( source_nodes1 )
		
		result = list( set0.union( set1 ) )
		
		self.querysets[ target ] = result
		
		return (True, len( result ))

	def intersection( self, source0, source1, target ):
		if source0 not in self.querysets:
			return (False, "Queryset (%s) not found." % source0 )
		
		if source1 not in self.querysets:
			return (False, "Queryset (%s) not found." % source1 )

		source_nodes0 = self.querysets[ source0 ]
		source_nodes1 = self.querysets[ source1 ]
		result = []
		
		set0 = set( source_nodes0 )
		set1 = set( source_nodes1 )
		
		result = list( set0.intersection( set1 ) )
		
		self.querysets[ target ] = result
		
		return (True, len( result ))

	def difference( self, source0, source1, target ):
		if source0 not in self.querysets:
			return (False, "Queryset (%s) not found." % source0 )
		
		if source1 not in self.querysets:
			return (False, "Queryset (%s) not found." % source1 )

		source_nodes0 = self.querysets[ source0 ]
		source_nodes1 = self.querysets[ source1 ]
		result = []
		
		set0 = set( source_nodes0 )
		set1 = set( source_nodes1 )
		
		result = list( set0.difference( set1 ) )
		
		self.querysets[ target ] = result
		
		return (True, len( result ))


	def find( self, key, value, operator, target ):
		if operator not in self.predicates:
			return (False, "Operator (%s) is not defined." % operator )
		
		predicate = self.predicates[ operator ]
		
		result = []
		
		for node_id in self.graph.nodes.keys():
			status, node = self.graph.get_node( node_id )
			if predicate( node[key], value):
				result.append( node_id )
				

		self.querysets[ target ] = result
		return (True, len( result ))
	
	
	def fetch( self, source ):
		if source not in self.querysets:
			return (False, "Queryset (%s) not found." % source )
		self.results.append( self.querysets[ source ] )
		#return (True, len( self.querysets[ source ] ) )
		return (True, self.querysets[ source ] )

	def clear( self, source ):
		if source not in self.querysets:
			return (False, "Queryset (%s) not found." % source )

		del self.queryset[ source ]
		
		return (True, "OK")
	

class Hawthorn( object ):
	def __init__( self ):
		self.graphs = {}
		for i in range( 16 ):
			self.graphs[i] = Graph()
		
		self.queries = {}
		self.next_query_id = 1
	
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
				return (True, "OK")
				
				
			elif op == 'DELETE':
				node_id = parse_int( params[0] )
				
				if not node_id:
					return (False, "Invalid node id (%s)." % params[0] )
				
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
				
				return query.graph.set_property( node_id, key, value )
				
				
			elif op == 'UNSET':
				if len( params ) != 2:
					return (False, "Invalid parameter count (%i), should be %i." % ( len(params), 2 ) )
				
				node_id = parse_int( params[0] )
				
				if not node_id:
					return (False, "Invalid node id (%s)." % params[0] )
				
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

			if op == 'EDGES':
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
				
				key = params[0]
				value = params[1]
				operator = params[2]
				target = params[3]
				
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
			print "Connection accepted"
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
				print sys.exc_info()
				print "Connection closed"
				
			
			self.end_query( qid )
			socket.close()
		
		return handler


	def run( self ):
		server = StreamServer( ('127.0.0.1', 7778), self.get_handler() )
		try:
			server.serve_forever()
		except KeyboardInterrupt:
			pass
		
		#server.kill()


hawt = Hawthorn()
hawt.run()
