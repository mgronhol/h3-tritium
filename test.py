#!/usr/bin/env python

from libs.HawthornProtocol import HawthornClient



hc = HawthornClient( "127.0.0.1", 7778 )

print hc.get( 1 )
print hc.edges(1)
