Prereqs
=======

- Install Java SDK
- Install [Maven](http://maven.apache.org/download.html) 

Building
========

Run:

    mvn install

Running the Examples
====================

In one terminal window run:

    java -cp target/example-1.0-SNAPSHOT.jar example.Publisher

In another terminal window run:

    java -cp target/example-1.0-SNAPSHOT.jar example.Listener

You can control to which stomp server the examples try to connect to by
setting the following environment variables: 

* `STOMP_HOST`
* `STOMP_PORT`
* `STOMP_USER`
* `STOMP_PASSWORD`
