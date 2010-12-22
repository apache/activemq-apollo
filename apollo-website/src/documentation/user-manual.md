# Apollo ${project_version} User Manual

{:toc:2-5}

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

### Adjusting JVM Settings

You can define the following environment variables in the `bin/apollo-broker`
start script to customize the JVM settings:

* `JAVACMD` : The path to the java executable to use
* `JVM_FLAGS` : The first JVM flags passed. Defaults to `-server -Xmx1G`, you
  may want to lower or raise the maximum memory based on your actual usage. 
* `APOLLO_OPTS` : Additional JVM options you can add
* `APOLLO_DEBUG` : Set to true to enabled debugging on port 5005
* `APOLLO_PROFILE` : Set to true to YourKit based profiling
* `JMX_OPTS` : The JMX JVM options used, defaults to 
  -Dcom.sun.management.jmxremote

Make sure you define the variables before the `apollo-broker` script
executes `apollo` and that the variables get exported in the case of the
unix script.

### Understanding the `apollo.xml` File

The simplest valid `apollo.xml` defines a single virtual host and a
single connector.

{pygmentize:: xml}
<broker xmlns="http://activemq.apache.org/schema/activemq/apollo">

  <virtual_host id="default">
    <host_name>localhost</host_name>
  </virtual_host>

  <connector id="tcp" bind="tcp://0.0.0.0:61613"/>
  
</broker>
{pygmentize}

The broker, virtual host, and connector are assigned a id which which
is used to by the REST based administration console to identify 
the corresponding runtime resource.  The virtual host will not persist
any messages sent to it since it does not have a configured messages 
store.

Brokers can be configured with multiple virtual hosts and connectors.

#### Connectors

A broker connector is used to accept new connections to the broker.
A `connector` element can be configured with the following attributes

* `enabled` : if set to false, then the connector host will be disabled.

* `bind` : The transport that the connector will listen on, it includes the
  ip address and port that it will bind to.

* `connection_limit` : The maximum number of concurrently open connections
  this connector will accept before it stops accepting additional
  connections.  If not set, then there is no limit.

* `protocol` : Defaults to `multi` which means that any of the broker's 
   supported protocols can connect via this transport.

Furthermore, the connector element may contain protocol specific
configuration elements. For example, to add have the broker set the `user_id`
header of messages to the id of user that sent the message, you would
use the following configuration:

{pygmentize:: xml}
<connector id="tcp" bind="tcp://0.0.0.0:61613">
  <stomp add_user_header="user_id"/>
</connector>
{pygmentize}

#### Virtual Hosts

A virtual hosts allows ${project_name} to support multi tenant style
configurations. Each virtual host is highly isolated each with it's own
persistence, security, and runtime constraints configuration.

Protocols like STOMP 1.1, inform the broker of which host the client is
attempting to connect with. The broker will search it's list of virtual hosts
to find the first host who has a configured `host_name` that matches.
Protocols which do NOT support virtual hosts, will just connect to the first
virtual host defined in the configuration file.

* `host_name` : a host name that the virtual host is known as.  This element
  should be repeated multiple times if the host has many host names.

A `virtual_host` element may be configured with the following attributes:

* `enabled` : if set to false, then the virtual host will be disabled.

* `purge_on_startup` : if set to true, the persistent state of the broker
   will be purged when the broker is started up.

The `virtual_host` can also define multiple `destination` and `queue` 
elements to secure or tune how message delivery works for different 
destinations or queues.  If none are defined, then sensible 
default settings are used which allow queue and destinations to be
auto created as they get accessed by applications.

Finally `virtual_host` configuration should also include a message store
configuration element to enable message persistence on the virtual host.

##### Destinations

A destination is a named routing path on the broker. You only need to define
a `destination` element if you want adjust the default configuration used for
your destination. When a new destination is first created in the broker, it's
configuration will be determined by the first `destination` which matches the
destination being created. The attributes matched against are:

* `name` : The name of the destination, you can use wild cards to match
  multiple 

A `destination` element may be configured with the following attributes:

* `unified` : If set to true, then routing then there is no difference
  between sending to a queue or topic of the same name.

* `slow_consumer_policy` : Valid values are `block` and `queue`. Defaults to
  `block`. This setting defines how topic subscriptions are handled which
  affects slow consumer scenarios. If set to `queue` then each subscription
  gets a temporary queue which can swap messages to disk when you have a slow
  consumer so that produces do not slow down to the speed of the slowest
  consumer. If set to `block`, the producers block on slow consumers which
  makes producers only as fast as the slowest consumer on the topic.

##### Queues

A queue is used to hold messages as they are being routed between
applications. You only need to define a `queue` element if you want adjust
the default configuration used for your queue. When a new queue is first
created in the broker, it's configuration will be determined by the first
`queue` which matches the queue being created. The attributes matched against
are:

* `name` : The name of the destination, you can use wild cards to match
  multiple.

* `kind` : Valid valuest are `ptp` for standard
  point to point queues or `ds` for durable subscriptions.

* `client_id` : Valid only if the `kind` is `ds`. This specify which client
  id this configuration should match.

* `subscription_id` : Valid only if the `kind` is `ds`. This specify which subscription
  id this configuration should match.

A `queue` element may be configured with the following attributes:

* `queue_buffer` : The amount of memory buffer space allocated for each queue.

* `producer_buffer` : The amount of memory buffer space allocated to each
producer for receiving messages.

* `consumer_buffer` : The amount of memory buffer space allocated to each
subscription for receiving messages.

* `persistent` : If set to false, then the queue will not persistently
store it's message.

* `swap` : If set to false, then the queue will not swap messages out of 
memory.

* `flush_range_size` : The number max number of flushed queue entries to load
  from the store at a time. Note that Flushed entires are just reference
  pointers to the actual messages. When not loaded, the batch is referenced
  as sequence range to conserve memory.
  
##### Unified Destinations

Unified destinations can be used so that you can mix queue and topic 
behavior on one logical destination.  For example, lets assumed `foo` 
is configured as a unified destination and you have 2 subscribers
on queue `foo` and 2 subscribers on topic `foo`, then when you publish to
queue `foo` or topic `foo`, the 2 queue subscribers will load balance
their messages and the 2 topic subscribers will each get a copy of the message.

It is important to note that the unified subscription will not start 
retaining it's messages in a queue until a queue subscriber subscribes from
it.

##### Message Stores

A message store is used to implement reliable messaging and message
swapping which are both optional features that disabled if no message
store is configured on the virtual host. If no message store is
configured, then all message routing is performed in memory and queue will
quickly "fill up" when you have slow or no consumers since the messages
cannot get swapped to disk.

${project_name} supports multiple message store implementations.  The 
implementations currently supported are:

* [BDB Store](#BDB_Store) : is a file based message store implemented using the 
  [Sleepycat BDB](http://en.wikipedia.org/wiki/Berkeley_DB) library.
  This is the most stable implementation.
* [HawtDB Store](#HawtDB_Store) : is a file based message store implemented using the 
  [HawtDB](http://hawtdb.fusesource.org/) library.  This implementation
  has known bugs and not recommend to be used unless your good with a 
  debugger.

###### BDB Store

Apache cannot redistribute the BDB library due to the terms of it's
license, but you can easily get a free copy directly from Oracle. Before
you can start using the BDB Store you must first download it from Oracle
at [je-4.1.6.jar](http://download.oracle.com/maven/com/sleepycat/je/4.1.6/je-4.1.6.jar)
and then copy it into the `${APOLLO_HOME}/lib` directory.

For those of you with curl installed, you can just run:

    curl http://download.oracle.com/maven/com/sleepycat/je/4.1.6/je-4.1.6.jar > ${APOLLO_HOME}/lib/je-4.1.6.jar

Once that is done, you can enable the store by adding a `bdb_store` element
inside your `virtual_host`.  Example:
    
{pygmentize:: xml}
  ...
  <virtual_host id="default">
    ...
    <bdb_store directory="${apollo.base}/data"/>
    ..
  </virtual_host>
  ...
{pygmentize}

A `bdb_store` element may be configured with the following attributes:

* `directory` : The directory which the store will use to hold it's data
  files. The store will create the directory if it does not already
  exist.
* `flush_delay` : The flush delay is the amount of time in milliseconds
  that a store will delay persisting a messaging unit of work in hopes
  that it will be invalidated shortly thereafter by another unit of work
  which would negate the operation.
* `read_threads` : The number of concurrent read threads to use when
  accessing the store. The value defaults to 10.

###### HawtDB Store

The HawtDB store implementation is redistributable by apache so it can 
be used out of the box without having to install any additional software.
The problem is that it is still unstable and you should only use it
if your willing to dive in with a debugger to help solidify the 
implementation.

You can enable the store by adding a `hawtdb_store` element
inside your `virtual_host`.  Example:
    
{pygmentize:: xml}
  ...
  <virtual_host id="default">
    ...
    <hawtdb_store directory="${apollo.base}/data"/>
    ..
  </virtual_host>
  ...
{pygmentize}

A `hawtdb_store` element may be configured with the following attributes:

* `directory` : The directory which the store will use to hold it's data
  files. The store will create the directory if it does not already
  exist.
* `flush_delay` : The flush delay is the amount of time in milliseconds
  that a store will delay persisting a messaging unit of work in hopes
  that it will be invalidated shortly thereafter by another unit of work
  which would negate the operation.

### Security

#### Using SSL/TLS

${project_name} supports SSL/TLS for transport level security to avoid 3rd
parties listening in on the communications between the broker and it's
clients. To enable it, you just need to add a connector which uses the `ssl`
or `tls` transport and add a `key_storage` configuration element under the
`broker` to configure the where the encryption keys and certificates are
stored.

Example:
{pygmentize:: xml}
  ...
  <key_storage 
     file="${apollo.base}/etc/keystore" 
     password="password" 
     key_password="password"/>
  <connector id="tls" bind="tls://0.0.0.0:61614"/>
  ...
{pygmentize}

The attributes that you can configure on the `key_storage` element are:

* `file` : Path to where the key store is located.
* `password` : The key store password
* `key_password` : The password to the keys in the key store.
* `store_type` : The type of key store, defaults to `JKS`.
* `trust_algorithm` : The trust management algorithm, defaults to `SunX509`.
* `key_algorithm` : The key management algorithm, defaults to `SunX509`.

#### Authentication

The first step to securing the broker is authenticating users. The default
${project_name} configurations use file based authentication. Authentication
is performed using JAAS who's config file is located in the instance
directory under `etc/login.conf`. JAAS configuration files can define
multiple named authentication domains. The `broker` element and
`virtual_host` elements can be configured to authenticate against these
domains.

The `authentication` element defined at the broker level will get used to 
authenticate broker level administration functions and to authenticate 
any virtual hosts which did not define an `authentication` element.  If you
want to disable authentication in a virtual host, you set the `enable` attribute
to `false`.

{pygmentize:: xml}
<broker xmlns="http://activemq.apache.org/schema/activemq/apollo">
  <authentication domain="internal"/>
  
  <virtual_host id="wine.com">
    <authentication domain="external"/>
    <host_name>wine.com</host_name>
  </virtual_host>

  <virtual_host id="internal.wine.com">
    <host_name>internal.wine.com</host_name>
  </virtual_host>

  <virtual_host id="test">
    <authentication enabled="false"/>
    <host_name>cheeze.com</host_name>
  </virtual_host>

  <connector id="tcp" bind="tcp://0.0.0.0:61613"/>
</broker>
{pygmentize}

The above example uses 2 JAAS domains, `internal` and `external`.  Broker 
The `wine.com` host will use the external domain, the `internal.wine.com`
host will use the internal domain and the `test` host will not authenticate
users.

##### Using Custom Login Modules

${project_name} uses JAAS to control against which systems users
authenticate. The default ${project_name} configurations use file based
authentication but with a simple change of the JAAS configuration, you can be
authenticating against local UNIX account or LDAP.  Please reference
the [JAAS documentation](http://download.oracle.com/javase/1.5.0/docs/guide/security/jaas/JAASRefGuide.html#AppendixB)
for more details on how to edit the `etc/login.conf` file.

Since different JAAS login modules produce principals of different class
types, you may need to configure which of those class types to recognize as
the user principal and the principal used to match against the access control
lists (ACLs). 

The default user principal class recognized is
`org.apache.activemq.jaas.UserPrincipal`. You can configure it by adding
`user_principal_kind` elements under the `authentication` element. The first
principal who's type matches this list will be selected as the user's
identity for informational purposes.

Similarly, default acl principal class recognized is
`org.apache.activemq.jaas.GroupPrincipal`. You can configure it by adding
`acl_principal_kinds elements under the `authentication` element. The ACL
entries which do not have an explicit kind will default to using the the
kinds listed here.

Example of customizing the principal kinds used:

{pygmentize:: xml}
  ...
  <authentication domain="apollo">
    <user_principal_kind>com.sun.security.auth.UnixPrincipal</user_principal_kind>
    <user_principal_kind>com.sun.security.auth.LdapPrincipal</user_principal_kind>
    <acl_principal_kind>com.sun.security.auth.UnixPrincipal</acl_principal_kind>
    <acl_principal_kind>com.sun.security.auth.LdapPrincipal</acl_principal_kind>
  </authentication>
  ...
</broker>
{pygmentize}

#### Authorization

User authorization to broker resources is accomplished by configuring an
access control list using an `acl` element on the `broker`, `connector`,
`virtual_host`, `destination`, or `queue` resources. The acl defines which
principals are allowed or denied access to perform actions against the
resources.  An example of `acl` is shown below:

{pygmentize:: xml}
<acl>
  <send deny="guest"/>
  <send allow="*"/>
  <receive allow="app1"/>
</acl>
{pygmentize}

If a configuration resource does not have an `acl` element defined within
it, then the resource allows anyone to access all it's actions. The `acl`
is made up of a list of authorization rule entries. Each entry defines
that action the rule applies to and if the rule is allowing or denying
access to a user principal. The special `*` value matches all users.

Users can have many principals of many different kinds associated with
them. The rules will only match up against principals of type
`org.apache.activemq.jaas.GroupPrincipal` since that is the default
setting of the `acl_principal_kind` of the `authentication` domain.

If you want the rule to match against more/different kinds of principals,
you should update the `authentication` element's configuration or you
explicitly state the kind you want to match against in your rule
definition. Example:

{pygmentize:: xml}
<acl>
  <send deny="chirino" kind="org.apache.activemq.jaas.UserPrincipal"/>
  <send allow="*"/>
</acl>
{pygmentize}

The order in which rule entries are defined are significant when the user
matches multiple entries. The first entry the user matches determines if he
will have access to the action. For example, lets say a user is groups
'blue' and 'red', and you are matching against an ACL list defined as:

{pygmentize:: xml}
<acl>
  <send deny="blue"/>
  <send allow="red"/>
</acl>
{pygmentize}

Then the user would not be allowed to send since `<send deny="blue"/>` was
defined first. If the order in the ACL list were reversed, like
so:

{pygmentize:: xml}
<acl>
  <send allow="red"/>
  <send deny="blue"/>
</acl>
{pygmentize}

Then the user would be allowed access to the resource since the first rule
which matches the user is `<send allow="red"/>`.

The type of resource being secured determines the types of actions that
can be secured by the acl rule entries. Here is listing of which actions
can be secured on which resources:

* `broker`
  * `admin` : use of the administrative web interface
* `connector` and `virtual_host`
  * `connect` : allows connections to the connector or virtual host
* `destination` and `queue`
  * `create` : allows the destination or queue to be created.
  * `destroy` : allows the destination or queue to be created.
  * `send` : allows the user to send to the destination or queue
  * `receive` : allows the user to send to do non_destructive read 
    from the destination or queue
* `queue`
  * `consume` : allows the user to do destructive reads against the queue.

#### Encrypting Passwords in the Configuration

The `etc/apollo.xml` file supports using `${<property-name>}` style
syntax. You can use any system properties and if the
`etc/apollo.xml.properties` file exists, then any of the properties
defined there. Any of the properties values in the
`etc/apollo.xml.properties` can be replaced with encrypted versions by
using the `apollo encrypt` command.

Lets say you your current `key_storage` contains plain text passwords that
need to be replaced with encrypted versions:

{pygmentize:: xml}
  ...
  <key_storage 
     file="${apollo.base}/etc/keystore" 
     password="open" 
     key_password="sesame"/>
  ...
{pygmentize}

Lets first find out what the encrypted versions of the passwords would be.
${project_name} encrypts and decrypts values using the password stored in
the `APOLLO_ENCRYPTION_PASSWORD` environment variable.

The following is an example of how you can encrypt the previous
passwords:

    $ export APOLLO_ENCRYPTION_PASSWORD='keepmesafe'
    $ apollo encrypt open
    ENC(6r7HKCib0H8S+OuSfV+muQ==)
    $ apollo encrypt sesame
    ENC(FP+H2FIg++sSaOxg/ISknw==)

Once you have the encrypted passwords, you can add them to the
`etc/apollo.xml.properties` file. Example:

    store.pass=ENC(6r7HKCib0H8S+OuSfV+muQ==)
    key.pass=ENC(FP+H2FIg++sSaOxg/ISknw==)

Finally the last step of securing the configuration is to replace the plain
text passwords with variable references to the corresponding property names:

{pygmentize:: xml}
  ...
  <key_storage 
     file="${apollo.base}/etc/keystore" 
     password="${store.pass}" 
     key_password="${key.pass}"/>
  ...
{pygmentize}

When you use encrypted passwords in your configuration, you MUST make
sure that the `APOLLO_ENCRYPTION_PASSWORD` environment variable is set
to the proper value before starting the broker.

### Web Based Administration

${project_name} start a web based administration interface on 
[`http://127.0.0.1:8080`](http://127.0.0.1:8080) by default.  Note
that it binds to the loopback interface so that only local web 
browsers can access the interface.

If the broker has authentication enabled and has defined an ACL
configuring the admins of the broker, then the web interface will 
perform basic authentication and will only grant access to those users
which are in the admin ACL.

If you want to change the port or the interface it binds on or perhaps
even disable it altogether, then you should add a `web_admin`
configuration element inside the `broker` element to change the
default settings. For example:

{pygmentize:: xml}
<broker xmlns="http://activemq.apache.org/schema/activemq/apollo">
  ...
  <web_admin host="127.0.0.1" port="8001"/>
  ...
</broker>
{pygmentize}

A `web_admin` element may be configured with the following attributes:

* `host` : The amount of memory buffer space allocated for each queue.
* `port` : The amount of memory buffer space allocated to each
* `prefix` : The prefix path to the web administration application
* `enabled` : If set to false then web administration is disabled. 

## Managing Brokers

### Starting a Broker Instance

Assuming you created the broker instance under `/var/lib/mybroker` all you
need to do start running the broker instance in the foreground is execute:

    /var/lib/mybroker/bin/apollo-broker run

### Stopping a Broker Instance

You can stop a running broker by executing: 

    /var/lib/mybroker/bin/apollo-broker stop --user admin --password password

This command uses the web administration interface to signal the broker
to shutdown.  If the that interface has been disabled you should just kill
the the broker process by killing it's process id using your operating
system's tools.

### Viewing Broker State

${project_name} provides a web based interface for administrators to inspect
the runtime state of the Broker.  If your running the broker on your local
machine, just open your web browser to [`http://localhost:8080`](http://localhost:8080).

The web interface will display the status of the the connectors and show
attached connections.  It will also allow you to drill into each configured
virtual host and view the destinations and queues being used. 

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

* [`http://stomp.github.com/implementations.html#Clients`](http://stomp.github.com/implementations.html#Clients)

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
connect.  If no configured virtual host `host_name` matches the client's 
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
the durable subscription using the `id` header on the `
SCRIBE` frame and
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

### Browsing Subscriptions

A normal subscription on a queue will consume messages so that no other
subscription will get a copy of the message. If you want to browse all the
messages on a queue in a non-destructive fashion, you can create browsing
subscription. Browsing subscriptions also works with durable subscriptions
since they are backed by a queue. To make a a browsing subscription, just add the
`browser:true` header to the `SUBSCRIBE` frame. For example:

    SUBSCRIBE
    id:mysub
    browser:true
    destination:/queue/foo
    
    ^@

Once the broker sends a browsing subscription the last message in the queue,
it will send the subscription a special "end of browse" message to indicate
browsing has completed and that the subscription should not expect any more
messages. The "end of browse" message will have a `browser:end` header set.
Example:

    MESSAGE
    subscription:mysub
    browser:end
    
    ^@

### Exclusive Subscriptions

We maintain the order of messages in queues and dispatch them to
subscriptions in order. However if you have multiple subscriptions consuming
from the same queue, you will loose the guarantee of processing the messages
in order; since the messages may be processed concurrently on different
subscribers.

Sometimes it is important to guarantee the order in which messages are
processed. e.g. you don't want to process the update to an order until an
insert has been done; or to go backwards in time, overwriting an newer update
of an order with an older one etc.

So what folks have to do in clusters is often to only run one consumer
process in a cluster to avoid loosing the ordering. The problem with this is
that if that process goes down, no one is processing the queue any more,
which can be problem.

${project_name} supports exclusive subscriptions which avoids the end user
having to restrict himself to only running one process. The broker will pick
a single subscription to get all the messages for a queue to ensure ordering.
If that subscription fails, the broker will auto failover and choose another
subscription.

An exclusive subscription is created by adding a `exclusive:true` header
to the `SUBSCRIBE` frame.  Example:

    SUBSCRIBE
    id:mysub
    exclusive:true
    destination:/queue/foo
    
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



