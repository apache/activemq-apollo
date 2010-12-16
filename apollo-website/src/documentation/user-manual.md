# Apollo ${project_version} User Manual

{:toc}

## Creating a Broker

A broker instance is the directory containing all the configuration and
runtime data such as logs and data files associated with a broker process. It
is recommended that you do *not* create the instance directory under the
directory where the ${project_name} distribution is installed.

On unix systems, it is a common convention to store this kind of runtime data
under the `/var/lib` directory. For example, to create an instance at
'/var/lib/mybroker', run:

    cd /var/lib
    apollo create mybroker

A broker instance directory will contain the following sub directories:

 * `bin` : holds execution scripts associated with this instance.
 * `etc` : hold the instance configuration files
 * `data` : holds the data files used for storing persistent messages
 * `log` : holds rotating log files
 * `tmp` : holds temporary files that are safe to delete between broker runs

At this point you may want to adjust the default configuration located in
etc directory.

## Broker Configuration

Each broker instance can be tuned and configured by editing one of the
following files.

* `bin/apollo-broker` : You can update the start script to control JVM level
  configuration options like JVM memory sizing.
* `etc/apollo.xml` : The primary configuration file for the broker. It
  controls the opened ports, the queues, security, virtual host settings and
  more.
* `etc/log4j.properties` : This is a standard 
  [log4j](http://logging.apache.org/log4j/1.2/manual.html) configuration file
  which controls the broker's logging.
* `etc/keystore` : A Java key store used to hold cryptographic security keys
  and certificates.  It is only need for brokers using SSL.
* `etc/login.conf` : A standard JAAS [login.conf] configuration file used to
  define the JAAS authentication domains available to the broker.
* `etc/users.properties` : Holds userid/password mappings of users that can 
  access the broker.  Referenced by the `etc/login.conf` file.
* `etc/groups.properties` : Holds groups to users mappings so that you can
  simplify access control lists (ACL) by using group instead listing individual
  users.
 
[login.conf]: http://download.oracle.com/javase/1.5.0/docs/guide/security/jaas/tutorials/LoginConfigFile.html

## Using the STOMP Protocol

Clients can connect to ${project_name} using the
[STOMP](http://stomp.github.com/) protocol. STOMP provides an interoperable
messaging wire level protocol that allows any STOMP clients can communicate
with any STOMP message broker. It aims to provide easy and widespread
messaging interoperability among many languages, platforms and brokers.

${project_name} supports the following versions of the STOMP specification: 

* [STOMP 1.0](http://stomp.github.com/stomp-specification-1.0.html)
* [STOMP 1.1](http://stomp.github.com/stomp-specification-1.1.html) *Not final*

The specification is short and simple to read, it is highly recommend that users
to get familiar with it before using one of the many available client libraries.

### Client Libraries

There are many open source STOMP clients for different platforms and
languages.  You can find a full listing of available clients at:

* http://stomp.github.com/implementations.html#Clients

The ${project_name} distribution ships with an `examples` directory
where you can find some simple examples of how to use some of those
clients to send and receive messages from a broker instance.

This section will focus on clarifying the parts of the STOMP specification
which allow servers to choose their behavior and to describe how to access
${project_name} specific features.

### Connecting

The default broker configuration secures access to the broker so that only
the `admin` user can connect.  The default password for the `admin` user 
is `password`.

Example STOMP frame to connect to the broker:

    CONNECT
    
    login:admin
    passcode:password
    
    ^@

STOMP 1.0 clients do specify which virtual host they are connecting to so
the broker connects those clients to the first virtual host defined in
it's configuration.  STOMP 1.1 clients do specify a virtual host when they 
connect.  If no configured virtual host `host-name` matches the client's 
requested host, the connection is terminated with an ERROR.  Therefore,
it is critical that the virtual hosts configuration define all the 
possible host names that clients may connect to host with.

### Destination Types

${project_name} supports two main types of destinations, queues and topics.
The most striking difference between queues and topics is how messages are 
delivered to consumers.  A queue will load it's messages over the connected
subscribers so that only one subscriber gets a message.  A topic follows the
publish/subscribe patterns and it's subscribers each get a copy of every
message sent.

If you want to send or subscribe to a queue or topic, the STOMP destination
should be prefixed with `/queue/` or `/topic/` respectively.

Example STOMP frame sending to a queue:

    SEND
    destination:/queue/a

    hello queue a
    ^@

Another major difference between queues and topics is that queues hold 
on to unconsumed messages even when there are no subscriptions attached,
while a topic will drop messages when there are no connected subscriptions.
${project_name} allows you to create Durable Subscriptions against topics
so that a subscription can "out live" the connection that created the 
subscription.  This allows you consume all the messages sent to the 
topic without messages getting dropped.

### Reliable Messaging

${project_name} supports reliable messaging by allowing messages to be
persisted so that they can be recovered if there is failure which kills
the broker.  Processing persistent messages has orders of magnitude more
overhead than non-persistent variety.  You should only use it if your
application really needs it.

To have a message be persistent, the sender must add the `persistent:true`
header to the `SEND` STOMP frame.  Furthermore, if you are not sending the 
message in a transaction, it is recommend that you also add a `receipt`
header.  Once the broker responds with a `RECEIPT` frame correlated to 
the send, you are guaranteed the broker will not drop the message even if
a failure occurs.

Example:

    SEND
    destination:/queue/a
    receipt:001

    hello queue a
    ^@

The client should not consider the send to have succeeded until he receives
the correlated `RECEIPT` frame from the server.  Example:

    RECEIPT
    receipt-id:001
    
    ^@

It is important to note that to do reliable messaging, your system will
be prone to receive duplicate messages.  For example, if your sending 
application dies before it receives the `RECEIPT` frame, then when it starts
up again will probably try to send the same message again which may
cause a duplicate message to get placed on the broker.

You should only use subscribers which use the `client` or `client-individual`
ack mode to consume reliable messages. Any messages on a queue delivered to a
client which have not been acked when the client disconnects will get
redelivered to another subscribed client.

### Topic Durable Subscriptions

A durable subscription is a queue which is subscribed to a topic so that
even if the client which created the durable subscription is not
online, he can still get a copy of all the messages sent to the topic
when he comes back online.  Multiple clients can subscribe to the same
durable subscription and since it's backed by a queue, those subscribers
will have the topic's messages load balanced across them.

To create or reattach to a a durable subscription with STOMP, you uniquely name
the durable subscription using the `id` header on the `SUBSCRIBE` frame and
also adding a `persistent:true` header. Example:

    SUBSCRIBE
    id:mysub
    persistent:true
    destination:/topic/foo
    
    ^@

A standard `UNSUBSCRIBE` frame does not destroy the durable subscription, it 
only disconnects the client from the durable subscription.  To destroy a 
durable subscription, you must once again add `persistent:true` header
to the `UNSUBSCRIBE` frame.  Example:

    UNSUBSCRIBE
    id:mysub
    persistent:true
    
    ^@

### Destination Wildcards

We support destination wildcards to easily subscribe to multiple destinations
with one subscription. This concept has been popular in financial market data
for some time as a way of organizing events (such as price changes) into
hierarchies and to use wildcards for easy subscription of the range of
information you're interested in.

* `.` is used to separate names in a path
* `*` is used to match any name in a path
* `**` is used to recursively match path names

For example imagine you are sending price messages from a stock exchange feed.
You might use some kind of destination naming conventions such as:

* `/topic/PRICE.STOCK.NASDAQ.IBM` to publish IBM's price on NASDAQ and
* `/topic/PRICE.STOCK.NYSE.SUNW` to publish Sun's price on the New York Stock Exchange

A subscriber could then use exact destinations to subscribe to exactly the
prices it requires. Or it could use wildcards to define hierarchical pattern
matches to the destinations to subscribe from.

For example using the example above, these subscriptions are possible

* `/topic/PRICE.**` : Any price for any product on any exchange
* `/topic/PRICE.STOCK.**` : Any price for a stock on any exchange
* `/topic/PRICE.STOCK.NASDAQ.*` : Any stock price on NASDAQ
* `/topic/PRICE.STOCK.*.IBM` : Any IBM stock price on any exchange

Destination wildcards can only be used in a SUBSCRIBE frame.

### Composite Destinations

You can use composite destinations to send or subscribe to multiple
destinations at one time. You use separator of `,` between destination
names.  For example, to send one message to 2 queues and 1 topic:

    SEND
    destination:/queue/a,/queue/b,/topic/c

    Composites rock!
    ^@

### Message Selectors

Message selectors allow a subscription to only receive a subset of the
messages sent to a destination.  The selector acts like a filter which
is applied against the message properties and only those messages
which match pass through to the subscription.  

Selectors are defined using SQL 92 syntax and typically apply to message
headers; whether the standard properties available on a JMS message or custom
headers you can add via the JMS code.

Here is an example:

    type = 'car' AND color = 'blue' AND weight > 2500

To create a subscription with a message selector, you set the `selector`
header in the STOMP `SUBSCRIBE` frame to the desired selector. Example:

    SUBSCRIBE
    id:sub0
    selector:type = 'car' AND color = 'blue' AND weight > 2500
    destination:/topic/foo
    
    ^@



