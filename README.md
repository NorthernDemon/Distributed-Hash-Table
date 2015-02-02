Distributed Hash Table
==============

Introduction
-------

Distributed Hash Table with Data Partitioning forms a ring topology with nodes and items. Application is build on top of Java RMI, which is an object-oriented equivalent of remote procedure calls (RPC).

####Features
    - server node can join or leave the ring
    - client can view topology of the ring, get or update the items

Installation
-------
Requirements: *JDK 7*, *Maven*

####To run inside of IDE:
    - run main ServerLauncher.java
    - run main ClientLauncher.java
    
Architecture Diagram
-------
![Diagram](/diagrams/Architecture_Diagram.png)

Authors
-------
[Dennis Eikelenboom](https://github.com/denniseik) and [Victor Ekimov](https://github.com/NorthernDemon)