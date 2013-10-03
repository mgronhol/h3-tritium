h3-tritium
==========

Hawthorn 3 graph database with Redis protocol interface


## Data model

Each node has an id (64 bit unsigned integer) and 0..N properties which are key-value pairs.

Nodes are connected to each other with edges. Edges have source, target, type and weight.

All edges are directional but for sake of convinience, the server also stores all incoming edges as "backward edges"
so that user can write simpler queries.

Edges have also a type (a string) and a weight (a string). These are used to characterize the connection between
the two nodes. All graph-traveling commands use type as a way to filter the results. Weight is currently just an 
application specific way to add more information on an edge.

### Querysets

In order to reduce the data flowing back and forth between the client and the server, the queries are 
written using querysets as a temporary storage for results. Each query takes a queryset as an input and
returns the results to an output queryset. Contents of a queryset can be fetched at any point during the connection.
Querysets are also connection specific, the are not visible to other connections to the server and are not
persisted between connections.



## Commands

`CREATE nodeID`

Create a new node with specified id.


`DELETE nodeID`

Remove a node from the graph. This also removes all edges associated with it.

`SET nodeID key value`

Sets a property on a specific node.

`UNSET nodeID key`

Removes a property from a node.

`GET nodeID`

Returns the properties from specified node.

`EDGES nodeID`

Returns both forward and backward edges from a node.

`CONNECT sourceID targetID type weight`

Connects two nodes with an edge from _source_ to _target_ with _type_ and _weight_.

`DISCONNECT sourceID targetID type`

Disconnect two nodes.

`START queryset nodeID0 nodeID1...`

Sets the _queryset_ to contain given nodes (1 or more).
This is can be used as a starting point to a query.

`FORWARD resultset sourceset type0 type1...`

Travel a step using forward edges (with given types).
Travels forward from each node from the source set and stores the results to the result set.

`BACKWARD resultset sourceset type0 type1...`

Same as FORWARD except that the travel is done using backward edges.

`UNION resultset sourcesetA sourcesetB`

Stores the union of source sets A and B to the result set.

`INTERSECTION resultset sourcesetA sourcesetB`

Stores the intersection of source sets A and B to the result set.

`DIFFERENCE resultset sourcesetA sourcesetB`

Stores the difference of source sets A and B to the result set.

`APPEND resultset sourcesetA sourcesetB`

Appends to querysets together. (This keeps the duplicates).

`FILTER resultset sourceset key value operator`

Filters a queryset with given (key, value, operator) configuration.

Currently supported operators:

``
  = node's property (indexed by key) must match the given value.
``

`FIND resultset key value operator`

Finds the nodes from the whole graph that match te (key, value, operator) configuration.

