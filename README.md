Distributed Hash Table with Data Partitioning
==============

Introduction
-------

DS project 2014/2015

LifestyleCoach is designed to help fatty lazy guys, like Matteo Matteovich, to get in shape! Our solution is web service oriented and opened for integration with other datasources.

####N-tier architecture includes
    - Client layer
    - Process layer
    - Service layer
    - Local datasource
    - External datasources

####Application is done using
    - Spring Framework
    - Hibernate
    - JSF
    - JPA
    - Log4J
    - Jackson
    - H2 in-memory database
    - Embedded Tomcat
    
####External datasource
    - Google Calendar
    - Facebook
    - Mashape
    - and much more! 

####Features
    - login with Facebook account to import personal data and post to the wall.
    - login with Google account to browse the calendar and create new event.
    - create Measure, Goal and MeasureType
    - view Goal, Awareness info, Motivation and Workout tips

Installation
-------
Requirements: *JDK 7*, *Maven*

Configure service parameters in **service.properties** file.

####To run inside of IDE:
    - ant compile
    - run main StandaloneServerLauncher.java
    - (optional test) run main StandaloneClientLauncher.java
    - http://{host}:{port} (example: http://localhost:9915)
    
####To run inside of Application Server:
    - ant create.war
    - deploy to Application Server
    - http://{host}:{port} (example: http://localhost:9915)

Use Case Diagram
-------
![Diagram](/diagrams/Use_Case_Diagram.png)

Entity Relationship Diagram
-------
![Diagram](/diagrams/Entity_Relationship_Diagram.png)

Architecture Diagram
-------
![Diagram](/diagrams/Architecture_Diagram.png)

Documentation
-------
Apiary: http://docs.lifestylecoach1.apiary.io/

Authors
-------
[Dennis Eikelenboom](https://github.com/denniseik) and [Victor Ekimov](https://github.com/NorthernDemon)