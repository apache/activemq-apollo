# The Apollo Project

## Synopsis

[Apollo][] is a forked and stripped down Apache [ActiveMQ][] message
broker. It is focused on simplicity, stability and scalability.

[Apollo]:http://github.com/chirino/activemq-apollo
[ActiveMQ]:http://activemq.apache.org/

## Implemented Features

* Topic style message routing
* Queue style message routing
* Persistent Messages
* Message swapping: moves message out of memory to support unlimited
  queue sizes
* Message Selectors
* Queue Browsers
* REST based management
* [Stomp](http://stomp.github.com/) Protocol Support

## What makes Apollo Different?

* [Architecture](apollo-website/src/architecture.md)
* [Performance and Scalability](apollo-website/src/performance-scaling.md)

## Building the Source Code

Prerequisites:

* [Maven >= 2.2.1](http://maven.apache.org/download.html)
* [Java JDK >= 1.6](http://java.sun.com/javase/downloads/widget/jdk6.jsp)

Then run:

    mvn install

## Quick Start 

We are still working on creating a binary distribution. Once that's created
we will update these instructions to work off that distribution. Until then,
they will be based on a built source distribution.

### Running an Apollo Broker

A broker with a web based admin interface will be started by using the the
Scala REPL console.

    $ cd apollo-web
    $ mvn -o scala:console 
    ... [output ommitted for brevity]
    scala> val main = org.apache.activemq.apollo.web.Main
    ... [output ommitted for brevity]
    scala> main run
    ... [output ommitted for brevity]
    Web interface available at: http://localhost:8080/

You can point your web browser at http://localhost:8080/ to explore the
management structure of the broker. Additional status objects will become
visible once there are connected client which cause connections and
destination management objects to be created.

### Running Examples

A stomp client will be started by using the the Scala 
repl console.

    $ cd apollo-stomp
    $ mvn -o scala:console 
    ... [output ommitted for brevity]
    scala> val client = org.apache.activemq.apollo.stomp.perf.StompLoadClient                 
    client: org.apache.activemq.apollo.stomp.perf.StompLoadClient.type = 
    --------------------------------------
    StompLoadClient Properties
    --------------------------------------
    uri              = stomp://127.0.0.1:61613
    destinationType  = queue
    destinationCount = 1
    sampleInterval   = 5000

    --- Producer Properties ---
    producers        = 1
    messageSize      = 1024
    persistent       = false
    syncSend         = false
    useContentLength = true
    producerSleep    = 0
    headers          = List()

    --- Consumer Properties ---
    consumers        = 1
    consumerSleep    = 0
    ack              = auto
    selector         = null

The above creates a client variable which allows you to customize all the
displayed properties. Those properties control how the client applies load
to the STOMP broker.  You could change the client configuration so that
it uses messages with 20 byte contents and send and receive on topics instead
of queues:

    scala> client.messageSize = 20
    scala> client.destinationType = "topic"


Once you are happy with the client configuration, you just run it and wait
for it to report back the producer and consumer throughput rates.

    scala> client.run                                                        
    =======================
    Press ENTER to shutdown
    =======================

    Producer rate: 155,783.906 per second, total: 778,960
    Consumer rate: 154,345.969 per second, total: 771,770
    Producer rate: 165,831.141 per second, total: 1,608,210
    Consumer rate: 165,798.734 per second, total: 1,600,858


