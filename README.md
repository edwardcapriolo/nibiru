nibiru
======

Nibiru is a NoSql database designed for maximum plugablitily and configurability. 

Motivation
======

There has been a recent proliferation in NoSql datastores. Many systems evolved around specific use cases. Many debates over design elements such as consistency model or implementations of specific features end with assertions that the design choice made is always the best for the user.

Nibiru takes a different approach by building API's throughout the codebase, not just stable client API. 

Building around APIs should allow users to swap physical backends or change routing semantics, by choosing between available implementations or designing one.

Quickstart
======

If you want to launch an instance of nibiru we have configuration files that will create a two node cluster. Run the following commands in two separate shells. You will find the server on port 7070 on 127.0.0.1 and 127.0.0.2.

    mvn exec:java -Dexec.mainClass="io.teknek.nibiru.Server" -Dexec.args="src/test/resources/nibiru_1.xml"
    mvn exec:java -Dexec.mainClass="io.teknek.nibiru.Server" -Dexec.args="src/test/resources/nibiru_2.xml"

