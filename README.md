Distributed Hash Table
==============

Introduction
-------

Distributed Hash Table with Data Partitioning forms a ring topology with nodes and items.

Application is build on top of Java RMI, which is an object-oriented equivalent of remote procedure calls (RPC).

####Features
    - server node can join or leave the ring
    - server can be crashed and recovered
    - server can be run on separate hosts
    - server supports replication of items
    - client can view topology of the ring
    - client can get/update items and replicas concurrently

####Assumptions
    - Nodes join, leave, crash or recover one at a time when there are no ongoing requests

Installation
-------
Requirements: *JDK 7*, *Maven*

Configure service parameters in **service.properties** file.

####To run inside of IDE:
    - mvn clean install
    - run main ServerLauncher.java
    - run main ClientLauncher.java
    
####To run as executable JAR:
    - mvn clean install
    - execute following line in new window to start the server:
        - java -jar DHT-${version}-server-jar-with-dependencies.jar
    - execute following line in new window to start the client:
        - java -jar DHT-${version}-client-jar-with-dependencies.jar

Authors
-------
[Dennis Eikelenboom](https://github.com/denniseik) and [Victor Ekimov](https://github.com/NorthernDemon)