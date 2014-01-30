#!/usr/bin/env python

import time, random, hashlib

def generate_id():
	return int( hashlib.sha1( str(random.random()+time.time()) ).hexdigest()[:15], 16 )


class HawthornEdges( object ):
	def __init__(self, conn, source_id, forward ):
		self._conn = conn
		self._source_id = source_id
		self._dir = forward
	
	def __getattr__( self, name ):
		if not name.startswith( "_" ):
			edges = self._conn.edges( self._source_id )
			if self._dir:
				return [HawthornNode(self._conn, x["target"]) for x in edges["forward"] if x["type"] == name]
			else:
				return [HawthornNode(self._conn, x["target"]) for x in edges["backward"] if x["type"] == name]
		
		return object.__getattribute__( self, name )
		
		
class HawthornNode( object ):
	_props = {}
	
	@staticmethod
	def find( conn, key, value, operator ):
		conn.find( 'r0', key, value, operator )
		nodes = conn.fetch( 'r0' )
		return [HawthornNode( conn, id ) for id in nodes]
	
	def __init__( self, *args, **kwargs ):
		self.id = False
		self._conn = args[0]
		self._props = {}
		self._hydrated = False
		self.forward = None
		self.backward = None
		
		if len( args ) > 1:
			self.id = args[1]
			self.forward = HawthornEdges( self._conn, self.id, True )
			self.backward = HawthornEdges( self._conn, self.id, False )
		else:
			new_id = generate_id() 
			while self._conn.get(new_id):
				new_id = generate_id()
			
			self.id = new_id
			self._conn.create( self.id )
		
		for (key, value) in kwargs.items():
			self._hydrated = True
			self._props[key] = value
			if key == "id":
				self.id = args[1]
				self.forward = HawthornEdges( self._conn, self.id, True )
				self.backward = HawthornEdges( self._conn, self.id, False )
		
		if not self.id:
			#self.id = 0 # tahan id:n generointi tai errori
			self.id = generate_id()
			while self._conn.get( self.id ):
				self.id = generate_id()
			self._conn.create( self.id )
			for (key,value) in self._props.items():
				self._conn.set( self.id, key, value )
	
	def edges(self):
		return self._conn.edges( self.id )
	
	def keys( self ):
		if not self._hydrated:
			response = self._conn.get( self.id )
			if "properties" in response:
				self._props = response["properties"]
				if self._props == []:
					self._props = {}
			
			self.forward = HawthornEdges( self._conn, self.id, True )
			self.backward = HawthornEdges( self._conn, self.id, False )
		
		return self._props.keys()
		
	def connect( self, target, edge_type ):
		self._conn.connect( self.id, target.id, edge_type, "" )
	
	def disconnect( self, target, edge_type ):
		self._conn.disconnect( self.id, target.id, edge_type )
	
	def setField( self, key, value ):
		self._props[key] = value
		#print self._props
		self._conn.set( self.id, key, value )
	
	def __str__( self ):
		return "Node(id = %i)" % self.id
	
	
	def __setattr__( self, name, value ):
		if name in self._props:
			self._props[name] = value
			self._conn.set( self.id, key, value )
		
		#print "setattr", name, value
		#props = self.__dict__['_props']
		#props = super(HawthornNode, self).__getattribute__('_props')
		
		
		object.__setattr__( self, name, value )

	def __getattr__( self, name ):
		if name in ["_props"]:
			raise AttributeError
		
		if not self._hydrated:
			response = self._conn.get( self.id )
			if "properties" in response:
				self._props = response["properties"]
				if self._props == []:
					self._props = {}
			
			self.forward = HawthornEdges( self._conn, self.id, True )
			self.backward = HawthornEdges( self._conn, self.id, False )
		
		#print "dbug:",self._props
		
		if name in self._props:
			return self._props[name]
		
		
		return object.__getattribute__( self, name )

				
