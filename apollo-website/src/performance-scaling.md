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
# Performance and Scaling

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