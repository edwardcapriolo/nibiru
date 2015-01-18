nibiru
======

Nibiru is a NoSql database designed for maximum plugablitily and configurability. 

Motivation
======

There has been a recent proliferation in NoSql datastores. Many systems evolved around specific use cases. Many debates over design elements such as consistency model or implementations of specific features end with assertions that the design choice made is always the best for the user.

Nibiru takes a different approach by building API's throughout the codebase, not just stable client API. 

Building around APIs should allow users to swap physical backends or change routing semantics, by choosing between available implementations or designing one.

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

