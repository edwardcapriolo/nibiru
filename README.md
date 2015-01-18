nibiru
======

Nibiru is a NoSql database designed for maximum plugablitily and configurability. 

Motivation
======

There has been a recent proliferation in NoSql datastores. Many systems evolved around specific use cases. Many debate over design elements like consistency model or implementations of specific features.

Nibiru takes a different approach by building API's throughout the codebase, not just stable client API. 

Building around api should allow users to swap physical backends or change routing semantics, by providing
easy to implemnt APIs.

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

