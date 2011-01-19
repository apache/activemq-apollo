Prereqs
=======

- Install [ActiveMQ-CPP](http://activemq.apache.org/cms/download.html) 

Building
========

This will vary depending on where you installed your libaries and the compiler 
you are using but on my Ubutu system, I compiled the examples as follows:

    gcc Listener.cpp -o listener -I/usr/local/include/activemq-cpp-3.2.4 -I/usr/include/apr-1.0 -lactivemq-cpp -lstdc++ 
    gcc Publisher.cpp -o publisher -I/usr/local/include/activemq-cpp-3.2.4 -I/usr/include/apr-1.0 -lactivemq-cpp -lstdc++ 

Running the Examples
====================

In one terminal window run:

    ./listener

In another terminal window run:

    ./publisher

You can control to which stomp server the examples try to connect to by
setting the following environment variables: 

* `STOMP_HOST`
* `STOMP_PORT`
* `STOMP_USER`
* `STOMP_PASSWORD`
