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

import json, os

class HawthornStorage( object ):
	def __init__( self ):
		pass
	
	def suppress( self, value ):
		pass
	
	def load( self, db ):
		pass
	
	def save( self, op, params ):
		pass



class AppendLogStorage( HawthornStorage ):
	def __init__( self, filename ):
		self.filename = filename
		self.handle = None
		self._suppress = False
	
	def suppress( self, value ):
		self._suppress = value
	
	def load( self, db ):
		
		if not os.path.exists( self.filename ):
			return
		
		qid = db.start_query( 0 )
		
		with open( self.filename, 'rb' ) as handle:
			for line in handle:
				data = json.loads(line)
				cmd = [data["op"]]
				cmd.extend( data["params"] )
				db.execute( qid, cmd )
	
		db.end_query( qid )
	
	def save( self, op, params ):
		
		if self._suppress:
			return
		
		if not self.handle:
			self.handle = open( self.filename, 'a' )
		
		data = json.dumps( {"op": op, "params": params } )
		self.handle.write( data  + "\r\n" )
