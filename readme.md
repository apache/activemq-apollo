# The Apollo Project - DEPRECATED

This project has died, and is now Deprecated, we strongly recommend you to use ActiveMQ 5.x or ActiveMQ Artemis.




## Synopsis

[ActiveMQ Apollo](http://activemq.apache.org/apollo/) is a faster, more
reliable, easier to maintain messaging broker built from the foundations of
the original [ActiveMQ](http://activemq.apache.org). It accomplishes this
using a radically different threading and message dispatching 
[architecture](documentation/architecture.html). 

In its current incarnation, Apollo only supports the STOMP protocol but just
like the original ActiveMQ, it’s been designed to be a multi protocol broker.
In future versions it should get OpenWire support so it can be compatible with
ActiveMQ 5.x JMS clients.

## Features

* [Stomp 1.0](http://stomp.github.com/stomp-specification-1.0.html) Protocol
  Support
* [Stomp 1.1](http://stomp.github.com/stomp-specification-1.1.html) Protocol
  Support
* [Topics and Queues](http://activemq.apache.org/apollo/documentation/user-manual.html#Destination_Types)
* [Queue Browsers](http://activemq.apache.org/apollo/documentation/user-manual.html#Browsing_Subscriptions)
* [Durable Subscriptions on Topics](http://activemq.apache.org/apollo/documentation/user-manual.html#Topic_Durable_Subscriptions)
* [Reliable Messaging](http://activemq.apache.org/apollo/documentation/user-manual.html#Reliable_Messaging)
* Message swapping
* [Message Selectors](http://activemq.apache.org/apollo/documentation/user-manual.html#Message_Selectors)
* [JAAS Authentication](http://activemq.apache.org/apollo/documentation/user-manual.html#Authentication)
* [ACL Authorization](http://activemq.apache.org/apollo/documentation/user-manual.html#Authorization)
* [SSL/TLS Support](http://activemq.apache.org/apollo/documentation/user-manual.html#Using_SSL_TLS)
* [REST Based Management](http://activemq.apache.org/apollo/documentation/architecture.html#REST_Based_Management)

## Documentation

 * [Getting Started Guide](http://activemq.apache.org/apollo/documentation/getting-started.html)
 * [User Manual](http://activemq.apache.org/apollo/documentation/user-manual.html)
 * [Management API](http://activemq.apache.org/apollo/documentation/management-api.html)
 
## Building the Source Code

Prerequisites:

* [Maven >= 3.0.2](http://maven.apache.org/download.html)
* [Java JDK >= 1.6](http://java.sun.com/javase/downloads/widget/jdk6.jsp)

Then run:

    mvn install -P download
    
This will build the binary distribution and place them in the
`apollo-distro/target` directory.


