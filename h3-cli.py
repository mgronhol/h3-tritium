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


import socket, sys

from libs.RedisProtocol import RedisProtocol

conn = socket.socket( socket.AF_INET, socket.SOCK_STREAM )

conn.connect( ('127.0.0.1', 7778 ) )

try:
	while True:
		line = sys.stdin.readline()
		parts = line.strip().split()
		
		msg = ""
		msg += "*%i\r\n" % len( parts )
		for part in parts:
			msg += "$%i\r\n" % len(part)
			msg += part + "\r\n"
		
		conn.send( msg )
		
		#print conn.recv(1024)
		rp = RedisProtocol( conn )
		print rp.receive()
		
		
except KeyboardInterrupt:
	pass



conn.close()
