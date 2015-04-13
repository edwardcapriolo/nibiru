nibiru
======

Nibiru is a NoSql database designed for maximum plugablitily and configurability. 

Motivation
======

There has been a recent proliferation in NoSql datastores. Many systems evolved from specific use cases or a specific industry. Often these systems are pitted against each other in apples to orange comparisons. Because apples and oranges can not be compared easily the debates typically degrade into a debate over what is the "best" implementation for the "real" users.

Nibiru takes a different approach by building API's throughout the codebase. There is no "locked in" implementation that is the "best" rather there is a system in place to provide alternative implementations. For example, Nibiru supports a gossiper for cluster discovery and a static provider. However this implemenentation is plugable so that an interested user can build a Zookeeper based implementation, an etcd based implementation, or whatever. 

Plugability and is the key focus throughout the codebase. Nibiru is not even built around a specific data model. We already have support for a Column Family data model and a Key Value data model! Again Nibiru does not aim to tell you "apples are the best" or "we only support oranges". A user should be able to plug together the pieces they desire into the system they want! For example if a user wants a fast, in memory, non durable, key value store for one table, and a strongly consistent ColumnFamily store in another table they should have be able to have that.


Quickstart
======

If you want to launch an instance of nibiru we have configuration files that will create a two node cluster. Run the following commands in two separate shells. You will find the server on port 7070 on 127.0.0.1 and 127.0.0.2.

    mvn exec:java -Dexec.mainClass="io.teknek.nibiru.Server" -Dexec.args="src/test/resources/nibiru_1.xml"
    mvn exec:java -Dexec.mainClass="io.teknek.nibiru.Server" -Dexec.args="src/test/resources/nibiru_2.xml"

There is a simple CLI that wraps a facade around many of the client functions in the database.

    mvn exec:java -Dexec.mainClass="io.teknek.nibiru.cli.Cli"  

    Welcome to a very minimal Cli type 'connect <host> <port>' to get started 
    ok> 
    connect localhost 7070
    ok>
 
