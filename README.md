nibiru
======

Nibiru is a NoSql database designed for maximum plugablitily and configurability. 

Motivation
======

Many NoSql datastores have involved from nitch use cases. These has debates of 
which solution is better based on semantics. From Nibiru's prospective the issue
is that many NoSql solutions offer very little pluggable API interfaces. 

Nibiru wants to build API's throughout the codebase, not just stable client API. This means
that it then should be easy to swap physical backends, or change routing semantics, by providing
easy to hack at API.

Implemented so far
======

Column Family Store:

* Compaction
* Indexes
* Memory tables
* SSTables

Coming soon
======
In Memory (no persistence) column family storage

Pondering
=====

Internode protocol
Coordinator protocol
Cluster Discovery (gossip)
Cluster Discovery (zookeeper)

