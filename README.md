Distributed Hash Table
==============

Introduction
-------

Distributed Hash Table with Data Partitioning forms a ring topology with nodes and items.

Application is build on top of Java RMI, which is an object-oriented equivalent of remote procedure calls (RPC).

####Features
    - server node can join or leave the ring
    - server supports replication of items
    - server can be crashed and recovered
    - client can view topology of the ring, get or update the items

Installation
-------
Requirements: *JDK 7*, *Maven*

####To run inside of IDE:
    - (optional) configure replication parameters in Replication.java
    - (optional) configure RMI port in ServerLauncher.java
    - run main ServerLauncher.java
    - run main ClientLauncher.java

Authors
-------
[Dennis Eikelenboom](https://github.com/denniseik) and [Victor Ekimov](https://github.com/NorthernDemon)