<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
  
       http://www.apache.org/licenses/LICENSE-2.0
  
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
  Architecture
-->
# Architecture

Apollo started as an experiment to see what it would take to make ActiveMQ
work better on machines with higher core counts. It has resulted in broker
that is much more deterministic, stable, and scaleable.

## Architectural Changes

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

### Scala 2.9 Implementation

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
services.

* A management client can easily be implemented in any language.
* There is very little management overhead since there is no special
  registration with the management system. The REST based management web
  application knows how to navigate the internal structure of a broker to
  access all the need status and statistics.
  
See the [Management API](management-api.html) documentation for
details.

### Message Swapping

Apollo can very efficiently work with both large and small queues due to
the way it implements message swapping. If you have a large queue with
millions of messages, and are slowly processing them, then it makes no
sense to keep all those messages in memory. They just need to be loaded
when the consumers are ready to receive them.

A queue in apollo has a configuration entry called `consumer_buffer` which
is the amount of memory dedicated to that consumer for prefetching into
memory the next set of messages that consumer will need. The queue will
asynchronously load messages from the message store so that they will be
in memory by the time the consumer is ready to receive the the message.

The rate of consumption/position of the consumers in the queue will also
affect how newly enqueued messages are handled. If no consumers are near
the tail of the queue where new messages are placed, then the message gets
swapped out of memory asap. If they consumers are near the tail of the
queue, then the message is retained in memory for as long as possible in
hopes that you can avoid a swap out and then back in.

When a message is swapped out of memory, it can be in one of 2 swapped out
states: 'swapped' or 'swapped range'. A message in 'swapped' state still
has a small reference node in the list of messages the queue maintains.
This small reference holds onto some accounting information about the
message and how to quickly retrieve the message from the message store.
Once a queue builds up many adjacent messages (defaults to 10,000) that
are in the 'swapped' state, it will replace all those individual reference
node entires in memory with a single range reference node. Once that
happens, the message is in a 'swapped range'.  




