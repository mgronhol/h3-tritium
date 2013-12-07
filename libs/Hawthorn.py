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
			if key in node["properties"]:
				if predicate( node["properties"][key], value):
					result.append( node_id )
			else:
				if key == "id":
					if predicate( hex(node["id"])[2:], value ):
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
