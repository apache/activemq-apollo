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

## Performance

Apollo's performance is awesome considering it is the text based protocol
Stomp. The source distribution includes a couple of benchmarks that are
useful for getting an idea of it's performance potential.

The benchmark clients access the server as follows:

* Clients and server run on the same machine. 

* Clients access the server using STOMP over TCP. 

* Producers send non-persistent messages

* Messages contain a 20 byte payload 

* Producers do not wait for a broker ack before sending the next message. 

* Consumers use auto ack.

The following benchmark results were run on a Mac Pro with:

* Dual socket 3 ghz Quad-Core Intel Xeon (8 total cores) with 12 MB L2
  cache per processor .

* 8 GB of RAM

### Queue Cases

* 1 producer sending to 1 consumer via 1 queue can hit rates of 250,000
  messages/second.

* 8 producers sending to 8 consumers via 8 queues can hit rates of 540,000
  messages/second.

* 1 producer sending to 10 consumers via 1 queue can hit rates of 230,000
  messages/second.

* 10 producers sending to 1 consumers via 1 queue can hit rates of 280,000
  messages/second.

### Topic Cases

Rates reported are the total consumer rate.

* 1 producer sending to 1 consumer via 1 topic can hit a rates of 420,000
  messages/second.

* 8 producers sending to 8 consumers via 8 topics can hit rates of 810,000
  messages/second.

* 10 producer sending to 10 consumers via 1 topic can hit rates of
  1,3000,000 messages/second.

## Scaling Characteristics

There are many different extreme ways that a messaging system can be used. 
Some folks like to:

* connect a large number of clients.

* hold a large number of messages in their queues.  

* move around large messages.

Apollo aims to support all those usage scenarios.

### Scaling the Number of Connected Clients

Apollo uses non blocking IO and a reactor thread model. This means that a
running broker uses a constant number of threads no matter how many clients
are connected to it.

### Scaling the Number of Queued Messages

Queues will swap messages out of memory when there are no consumers that
are likely to need the message soon. Once a message is swapped, the queue
will replace the message with a reference pointer. When the queue builds a
large number (more than 10,000) of these swapped out reference pointers,
they then get consolidated into a single "range entry" and the pointers are
dropped. 

When a consumer comes along that needs some of the swapped out messages,
it will convert previously consolidated "range entries" to a set of
reference pointers. The next reference pointers that the consumer is
interested in get swapped back as regular messages. This allows Apollo's
queues to hold millions of messages without much impact on JVM memory
usage.

### Scaling the Message Size

Big messages don't even make it into the JVM's memory management. The
contents of big messages received and sent using buffers allocated in a
memory mapped file. This allows the OS to efficiently manage swapping those
large messages to disk and it avoids churning the eden garbage collection
generation in the JVM. If 1 producer is sending 100 MB messages to a
consumer the JVM will not report any significant memory usage.

## Building the Source Code

TODO

## Quick Start 

### Running an Apollo Broker

TODO

### Running Examples

TODO

## Architectural Changes

Apollo started as an experiment to see what it would take to make ActiveMQ
work better on machines with higher core counts. It has resulted in broker
that is much more deterministic, stable, and scaleable.

The major fundamental architectural changes it brings are:

* Reactor Based Thread Model
* Scala 2.8 Implementation
* Protocol Agnostic
* REST Based Management

### Reactor Based Thread Model

Apollo uses [HawtDispatch][] to implement the sever using a multi-threaded
non-blocking variation of the [reactor][] design pattern. HawtDispatch is a
Java clone of [libdispatch][] (aka [Grand Central Dispatch][gcd]). It uses a
fixed sized thread pool and only executes non-blocking tasks on those
threads.

[reactor]:http://en.wikipedia.org/wiki/Reactor_pattern
[libdispatch]:http://en.wikipedia.org/wiki/Libdispatch
[HawtDispatch]:http://hawtdispatch.fusesource.org/
[gcd]:http://images.apple.com/macosx/technology/docs/GrandCentral_TB_brief_20090903.pdf


The thread model allows Apollo to reach very high levels of scaleability and
efficiency, but it places a huge restriction on the developer: all the tasks
it executes must be non-blocking and ideally lock-free and wait free. This
means that the previous ActiveMQ broker architecture had to go through a
major overhaul. All synchronous broker interfaces had to be changed
so that they would instead return results via asynchronous callbacks.

### Scala 2.8 Implementation

Even though Apollo started as a fork of ActiveMQ 5.x, the new reactor design
restrictions required major changes from the network IO handling, to the
flow control design, all the way to the Store interfaces. Scala provided a
much more concise way to express callbacks, namely by it's support for
partial functions and closures.

### Protocol Agnostic

ActiveMQ has supported multiple protocols for many years, but under the
covers what it's doing is converting all the protocols to use the
[OpenWire][] messaging protocol. Naturally, this approach is not optimal if
you want to efficiently support other protocols and not be limited by what
OpenWire can support.

[OpenWire]:http://activemq.apache.org/openwire.html

The Apollo server is much more modular and protocol agnostic. All protocols
are equal and are built as a plugin to the the broker which just make use of
exposed broker services for routing, flow control, queueing services etc.
For example, this means that messages will be persisted in a Store in the
original protocol's encoding. There is no protocol conversion occurring
under the covers unless it is required.

### REST Based Management

ActiveMQ choose to exposed it's management interfaces via JMX. JMX was a
natural choice since ActiveMQ is Java based, but JMX has a couple of
serious limitations:

* No cross language support

* Not scaleable for exposing many objects. Registering and unregistering
  management objects in JMX can become a bottle neck.

* Rich data types are hard to expose

Apollo exposes a rich and detailed state of the sever using REST based JSON
or XML services.

* A management client can easily be implemented in any language.

* There is very little management overhead since there is no special
  registration with the management system. The JAX-RS based management web
  application knows how to navigate the internal structure of a broker to
  access all the need status and statistics.



