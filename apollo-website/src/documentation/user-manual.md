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
* `black-list.txt` : A list of IP address which are banned from connecting 
  to the broker.

[login.conf]: http://download.oracle.com/javase/1.5.0/docs/guide/security/jaas/tutorials/LoginConfigFile.html

### Automatic Configuration Reloading

Once a broker is started, you can edit any of the configuration files in
the `etc` directory and your changes will be automatically reloaded.  The
configuration update will be applied in the least non-disruptive way possible.
For example, if you removed a connector, the port that connector was listening
on will be released and no new connections will be accepted. Connections that
were previously accepted by that connector will continue to operate normally.

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

There are many XSD aware XML editors which make editing XML configuration
files less error prone.  If your using one of these editors, you can
configure it to use this [apollo.xsd](schema/apollo.xsd) file.

The simplest valid `apollo.xml` defines a single virtual host and a
single connector.

{pygmentize:: xml}
<broker xmlns="http://activemq.apache.org/schema/activemq/apollo">

  <virtual_host id="default">
    <host_name>localhost</host_name>
    <null_store/>
  </virtual_host>

  <connector id="tcp" bind="tcp://0.0.0.0:61613"/>
  
</broker>
{pygmentize}

The broker, virtual host, and connector are assigned an id which which
is used to by the REST based administration console to identify 
the corresponding runtime resource.  The virtual host will not persist
any messages sent to it since it is using the `null_store`.

Brokers can be configured with multiple virtual hosts and connectors.

When a broker is first started up, it will validate the configuration
file against the the [XSD Schema](schema/apollo.xsd) and report any 
errors/warnings it finds but it will continue to start the broker even 
it finds problems.  You would want to the broker to abort starting up
if any issues are found with the schema validation you should set
the `broker` element's `validation` attribute to `strict`.  Example:

{pygmentize:: xml}
<broker validation="strict" 
   xmlns="http://activemq.apache.org/schema/activemq/apollo">
  ...
</broker>
{pygmentize}

#### Connectors

A broker connector is used to accept new connections to the broker.
A `connector` element can be configured with the following attributes

* `enabled` : if set to false, then the connector host will be disabled.

* `bind` : The transport that the connector will listen on, it includes the
  ip address and port that it will bind to.  Transports are specified using 
  a URI syntax.

* `connection_limit` : The maximum number of concurrently open connections
  this connector will accept before it stops accepting additional
  connections.  If not set, then there is no limit.

* `protocol` : Defaults to `any` which means that any of the broker's 
   supported protocols can connect via this transport.

Furthermore, the connector element may contain protocol specific
configuration elements. For example, to have the broker set the `user_id`
header of messages to the id of user that sent the message, you would
use the following configuration:

{pygmentize:: xml}
<connector id="tcp" bind="tcp://0.0.0.0:61613">
  <stomp add_user_header="user_id"/>
</connector>
{pygmentize}

##### TCP Transports

The TCP transport uses the `tcp://` URI scheme.  It uses the URI host
and port to determine to which local interfaces to bind.  For example:

* `tcp://0.0.0.0:61613` binds to all IPv4 interfaces on port 61613
* `tcp://[::]:61613` binds to all IPv4 and IPv6 interfaces on port 61613
* `tcp://127.0.0.1:0` binds to the loopback interface on a dynamic port

The TCP URI also supports several query parameters to fine tune the
settings used on the socket.  The supported parameters are:

* `receive_buffer_size` : Sets the size of the internal socket receive 
   buffer.  Defaults to 65536 (64k)

* `send_buffer_size` : Sets the size of the internal socket send buffer.  
   Defaults to 65536 (64k)

* `keep_alive` : Enable or disable the SO_KEEPALIVE socket option.  
   Defaults to true.

* `traffic_class` : Sets traffic class or type-of-service octet in the IP 
  header for packets sent from the transport.  Defaults to `8` which
  means the traffic should be optimized for throughput.

* `max_read_rate` : Sets the maximum bytes per second that this transport will
  receive data at.  This setting throttles reads so that the rate is not exceeded.
  Defaults to 0 which disables throttling.

* `max_write_rate` : Sets the maximum bytes per second that this transport will
  send data at.  This setting throttles writes so that the rate is not exceeded.
  Defaults to 0 which disables throttling.
  
Example which uses a couple of options:

{pygmentize:: xml}
<connector id="tcp" bind="tcp://0.0.0.0:61613?receive_buffer_size=1024&amp;max_read_rate=65536"/>
{pygmentize}

Note that `&amp;` was used to separate the option values instead of just `&` since the 
URI being written within an XML file.

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

The `virtual_host` can also define multiple `topic`, `queue`, and
`dsub` elements to secure or tune how message delivery works
for different topics or queues. If none are defined, then sensible default
settings are used which allows destinations to be auto created as they get
accessed by applications.

Finally `virtual_host` configuration should also include a message store
configuration element to enable message persistence on the virtual host.

##### Queues

When a new queue is first created in the broker, it's configuration will be
determined by the first `queue` element which matches the queue being
created. The attributes matched against are:

* `id` : The name of the queue, you can use wild cards to match
  multiple or don't set to match all queues.

If the queue definition is not using a wild card in the id, then the
queue will be created when the broker first starts up.

A `queue` element may be configured with the following attributes:

* `unified` : If set to true, then routing then there is no difference
  between sending to a queue or topic of the same name.  See the
  [Unified Destinations](#Unified_Destinations) documentation for more 
  details.  Defaults to false.

* `queue_buffer` : The amount of memory buffer space allocated for each queue.
   Defaults to 32k.

* `consumer_buffer` : The amount of memory buffer space allocated to each
subscription for receiving messages.  Defaults to 256k.

* `persistent` : If set to false, then the queue will not persistently
store it's message.  Defaults to true.

* `swap` : If set to false, then the queue will not swap messages out of 
memory.  Defaults to true.

* `swap_range_size` : The number max number of flushed queue entries to load
  from the store at a time. Note that Flushed entires are just reference
  pointers to the actual messages. When not loaded, the batch is referenced
  as sequence range to conserve memory.  Defaults to 10000.

* `quota` : The maximum amount of disk space the queue is allowed
  to grow to.  If set to -1 then there is no limit.  You can
  use settings values like: `500mb` or `1g` just plain byte count
  like `1024000`.  Once the quota is Exceeded, the producers will
  block until the usage decreases.  Defaults to no limit.
  
* `auto_delete_after`: If not set to `0` then the queue will automatically
  delete once there have been no consumers, producers or messages on it
  for the configured number of seconds.  Defaults to 300 if not set.

##### Topics

When a new topic is first created in the broker, it's configuration will be
determined by the first `topic` element which matches the topic being
created. The attributes matched against are:

* `id` : The name of the topic, you can use wild cards to match
  against multiple or don't set to match all topics.

If the topic definition is not using a wild card in the id, then the
topic will be created when the broker first starts up.

A `topic` element may be configured with the following attributes:

* `slow_consumer_policy` : Valid values are `block` and `queue`. Defaults to
  `block`. This setting defines how topic subscriptions are handled which
  affects slow consumer scenarios. If set to `queue` then each subscription
  gets a temporary queue which can swap messages to disk when you have a slow
  consumer so that produces do not slow down to the speed of the slowest
  consumer. If set to `block`, the producers block on slow consumers which
  makes producers only as fast as the slowest consumer on the topic.

* `auto_delete_after`: If not set to `0` then the topic will automatically
  delete once there have been no consumers or producers on it
  for the configured number of seconds.  Defaults to 300 if not set.

##### Durable Subscriptions

When a new durable subscription is first created in the broker, it's
configuration will be determined by the first `dsub` element
which matches the durable subscription being created. The attributes matched
against are:

* `id` : The name of the subscription.
* `id_regex` : A regular expression used to match against the subscription id

If you want to create the durable subscription when the broker first starts up,
then you must set the `topic` attribute and optionally the `selector` attribute.

* `topic` : The topic which the durable subscription will subscribe to.
* `selector` : A selector expression which filters out messages

A `dsub` element may be configured with all the 
attributes available on the `queue` element.

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
* [JDBM2 Store](#JDBM2_Store) : is a file based message store implemented using the 
  [JDBM2](http://code.google.com/p/jdbm2/) library.
  This is the most stable implementation.

<!-- 
* [HawtDB Store](#HawtDB_Store) : is a file based message store implemented using the 
  [HawtDB](http://hawtdb.fusesource.org/) library.  This implementation
  has known bugs and not recommend to be used unless your good with a 
  debugger. 
-->

###### BDB Store

Apache cannot redistribute the BDB library due to the terms of it's
license, but you can easily get a free copy directly from Oracle. Before
you can start using the BDB Store you must first download it from Oracle
at [je-4.1.10.jar](http://download.oracle.com/maven/com/sleepycat/je/4.1.10/je-4.1.10.jar)
and then copy it into the `${APOLLO_HOME}/lib` directory.

For those of you with curl installed, you can just run:

    curl http://download.oracle.com/maven/com/sleepycat/je/4.1.10/je-4.1.10.jar > ${APOLLO_HOME}/lib/je-4.1.10.jar

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

###### JDBM2 Store

The JDBM2 store implementation is redistributable by apache so it can 
be used out of the box without having to install any additional software.
The problem with this store is that it's single threaded in nature and
subsequently has several unwanted performance problems.  For example,
the store periodically will compact itself to reclaim disk space but
the compaction is a slow process which pauses access to the store while 
the compaction is running. 

You can enable the store by adding a `jdbm2_store` element
inside your `virtual_host`.  Example:
    
{pygmentize:: xml}
  ...
  <virtual_host id="default">
    ...
    <jdbm2_store directory="${apollo.base}/data"/>
    ..
  </virtual_host>
  ...
{pygmentize}

A `jdbm2_store` element may be configured with the following attributes:

* `directory` : The directory which the store will use to hold it's data
  files. The store will create the directory if it does not already
  exist.
* `flush_delay` : The flush delay is the amount of time in milliseconds
  that a store will delay persisting a messaging unit of work in hopes
  that it will be invalidated shortly thereafter by another unit of work
  which would negate the operation.
* `compact_interval` : how often the store is compacted in seconds.  If 
  this attribute is not set, it will default to 60 seconds. Set to -1 
  to disable compaction altogether.

<!--  
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

-->

### Security

#### The SSL/TLS Transport

${project_name} supports SSL/TLS for transport level security to avoid 3rd
parties listening in on the communications between the broker and it's
clients. To enable it, you just need to add a connector which binds using
on of the secure transports such as `ssl://`.  It also requires having a 
`key_storage` configuration element under the `broker` to configure the where 
the encryption keys and certificates are stored.

Example:
{pygmentize:: xml}
  ...
  <key_storage 
     file="${apollo.base}/etc/keystore" 
     password="password" 
     key_password="password"/>
  
  <connector id="stomp-secure" bind="ssl://0.0.0.0:61614"/>
  ...
{pygmentize}

The `connector` element's `bind` attribute controls which secure transport 
algorithm gets used by the sever.  Supported values are:

* `ssl://`    - Supports some version of SSL
* `sslv2://`  - Supports SSL version 2 or higher
* `sslv3://`  - Supports SSL version 3
* `tls://`    - Supports some version of TLS
* `tlsv://`   - Supports RFC 2246: TLS version 1.0 
* `tls1.1://` - Supports RFC 4346: TLS version 1.1 

The attributes that you can configure on the `key_storage` element are:

* `file` : Path to where the key store is located.
* `password` : The key store password
* `key_password` : The password to the keys in the key store.
* `store_type` : The type of key store, defaults to `JKS`.
* `trust_algorithm` : The trust management algorithm, defaults to `SunX509`.
* `key_algorithm` : The key management algorithm, defaults to `SunX509`.

The SSL/TLS transport is an extension of the TCP transport and as such it supports
all the same URI options which the TCP transport supports.

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

The default user principal classes recognized are
`org.apache.activemq.jaas.UserPrincipal` and
`javax.security.auth.x500.X500Principal`. You can change the default by
adding `user_principal_kind` elements under the `authentication` element.
The first principal who's type matches this list will be selected as the
user's identity for informational purposes.

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
access control rules using a `access_rule` elements in the `broker` or
`virtual_host` elements. The rules defines which principals are allowed or
denied access to perform actions against server resources. An example list of
rule is shown below:

{pygmentize:: xml}
<broker>
  <access_rule deny="guest" action="send"/>
  <access_rule allow="*"    action="send"/>
  <access_rule allow="app1" action="receive"/>
</broker>
{pygmentize}


The `allow` and `deny` attributes define the principals which are allowed or
denied access. If set to "+" then it matches all principals but requires at
at least one. If set to "*" the it matches all principals and even matches
the case where there are no principals associated with the subject.

Either `allow` or `deny` must be defined. You can optionally define one or
more of the following attributes to further narrow down when the rule matches
an authorization check:

* `separator`: If set, then the `allow` and `deny` fields will be interpreted
  to be a list of principles separated by the separator value.

* `principal_kind`: A space separated list of class names of which will be
  matched against the principle type. If set to `*` then it matches all
  principal classes. Defaults to the default principal kinds configured on
  the broker or virtual host.

* `action`: A space separated list of actions that will match the rule.
  Example 'create destroy'. You can use `*` to match all actions.  Defaults
  to `*`.

* `kind`: A space separated list of broker resource types that will match
  this rule. You can use `*` to match all key. Example values 'broker queue'.
  Defaults to `*`.

* `id`: The identifier of the resource that will match this rule. You can use
  `*` to match all resources. If the `kind` is set to `queue` or `topic` the
  your can use a destination wild card to match against the destination id.
  Defaults to `*`

* `id_regex`: A regular expression to be applied against the id of the
  resource.

If no access rules match an authorization check then access is denied. 

##### Ordering

The order in which rules are defined are significant. The first entry that
matches determines if he will have access. For example, lets say a user is
groups 'blue' and 'red', and you are matching against the following 
rules:

{pygmentize:: xml}
<access_rule deny="blue" action="send"/>
<access_rule allow="red" action="send"/>
{pygmentize}

Then the user would not be allowed to send since the deny rule was
matched first. If the order in the ACL list were reversed, like
so:

{pygmentize:: xml}
<access_rule allow="red" action="send"/>
<access_rule deny="blue" action="send"/>
{pygmentize}

Then the user would be allowed access to the resource since the allow
rule matched first.  When a single rule defines both `allow` and
`deny` attributes and they both match then the action is denied.

{pygmentize:: xml}
<access_rule deny="blue" allow="red" action="send"/>
{pygmentize}

#### Resource Actions

You can configure the `action` attribute of an access rules with
one or more of the following values:

* `admin` : use of the administrative web interface
* `monitor` : read only use of the administrative web interface
* `config` : use of the administrative web interface to access and change the
  broker configuration.
* `connect` : allows connections to the connector or virtual host
* `create` : allows creation
* `destroy` : allows destruction
* `send` : allows the user to send to the destination
* `receive` : allows the user to send to do non-destructive reads from the
  destination
* `consume` : allows the user to do destructive reads against a destination
* `*` : All actions

#### Resource Kinds

You can configure the `kind` attribute of an access rules with one or more of
the following values: `broker`, `connector`, `virtual_host`, `topic`,
`queue`, `dsub`, or `*`. `*` matches all resource kinds.

The `broker` and `connector` kinds can only be configured in rules
defined in the `broker` element.

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

${project_name} starts a web based administration interface on 
[`http://127.0.0.1:61680`](http://127.0.0.1:61680) and 
[`https://127.0.0.1:61681`](https://127.0.0.1:61681) by default.  Note
that it binds to the loopback interface so that only local web 
browsers can access the interface.

If the broker has authentication enabled and has defined an ACL
configuring the admins of the broker, then the web interface will 
perform basic authentication and will only grant access to those users
which are in the admin ACL.

If you want to disable the web the interface then you should remove
the `web_admin` configuration elements.  If you want to allow 
remote administration, you should update the configuration so
it bind either the `0.0.0.0` or `[::]` address.  

For example:

{pygmentize:: xml}
<broker xmlns="http://activemq.apache.org/schema/activemq/apollo">
  ...
  <web_admin bind="http://0.0.0.0:61680"/>
  <web_admin bind="https://0.0.0.0:61681"/>
  ...
</broker>
{pygmentize}

A `web_admin` element may be configured with the following attributes:

* `bind` : The address and port to bind the web interface on in URL syntax.

## Managing Brokers

The rest of this section's example assume that you have created a broker 
instance under the `/var/lib/mybroker` directory or `c:\mybroker` directory
if your on windows.

### Running a Broker Instance in the Foreground

To start the broker instance in the foreground simply execute
`bin/apollo-broker run`. Example:

    /var/lib/mybroker/bin/apollo-broker run

To stop it, press `Ctrl-C` to send the termination signal
to the process.

### Managing a Background Broker Instance

#### On Linux/Unix

If you are on Unix, you can use the `bin/apollo-broker-service` script
to manage the broker service.  This script is compatible with the
`/etc/init.d` style scripts most Unix/Linux systems use to control
background services.

On a Ubuntu OS, you install the service and have it run on start 
up by running:

    sudo ln -s /var/lib/mybroker/bin/apollo-broker-service /etc/init.d/apollo
    sudo update-rc.d apollo defaults

On a Redhat OS, you install the service and have it run on start 
up by running:

    sudo ln -s /var/lib/mybroker/bin/apollo-broker-service /etc/init.d/apollo
    sudo chkconfig apollo --add

On other Unixes, you install the service and have it run on start 
up by running:

    sudo ln -s /var/lib/mybroker/bin/apollo-broker-service /etc/init.d/apollo
    sudo ln -s /etc/init.d/apollo /etc/rc0.d/K80apollo
    sudo ln -s /etc/init.d/apollo /etc/rc1.d/K80apollo
    sudo ln -s /etc/init.d/apollo /etc/rc3.d/S20apollo
    sudo ln -s /etc/init.d/apollo /etc/rc5.d/S20apollo
    sudo ln -s /etc/init.d/apollo /etc/rc6.d/K80apollo

You can directly use the script to perform the following functions:

* starting: `apollo-broker-service start`
* stopping: `apollo-broker-service stop`
* restarting: `apollo-broker-service restart`
* checking the status: `apollo-broker-service status`

When the broker is started in the background, it creates
a `data/apollo.pid` file which contains the process id of
the process executing the broker.  This file is typically 
used to integrate with an external watch dog process
such as [Monit](http://mmonit.com/monit/).

#### On Windows

Windows users can install the broker as background service using the
`bin\apollo-broker-service` executable.

To install as a background service you would execute:

    c:\mybroker\bin\apollo-broker-service install

Uninstalling the service is done using:

    c:\mybroker\bin\apollo-broker-service uninstall
    
You can now use the standard Windows service tools to control
starting and stopping the broker or you can also use the
same executable to:

* start the service: `apollo-broker-service start`
* stop the service: `apollo-broker-service stop`
* check the status: `apollo-broker-service restart`

If you want to customize the JVM options use to start the background
service, you will need to edit the `bin\apollo-broker-service.xml`
file.  All service console and event logging perform by the background
service are stored under `log\apollo-broker-service.*`.

### Viewing Broker State

${project_name} provides a web based interface for administrators to inspect
the runtime state of the Broker.  If your running the broker on your local
machine, just open your web browser to [`http://localhost:61680`](http://localhost:61680)
or [`https://localhost:61681`](httsp://localhost:61681).

The web interface will display the status of the the connectors and show
attached connections.  It will also allow you to drill into each configured
virtual host and view the topics and queues being used. 

Please see the [Management API](management-api.html) documentation for more 
information on how to use the web based interface as a RESTful API.

### Exporting/Importing Stores

Exporting compresses all the data in a virtual host's message store in a zip
archive. Importing reverses the export process and restore the exported data
back to a virtual host's message store. Exporting/Importing is typically used
to:

* backup a message store
* migrate between different message store implementations

The stores which support exporting and importing are:

* [BDB Store](#BDB_Store) 
* [JDBM2 Store](#JDBM2_Store)

The broker must be stopped before using the import or export commands. Be
careful when importing an archive, since it will first purge the message
store of any data! 

Use the `apollo-broker store-export` command to export the data. For example:

    /var/lib/mybroker/bin/apollo-broker store-export myarchive.zip

The above command will load the `mybroker`'s configuration and export the
first virtual host's messages store to the `myarchive.zip`. You can use the
`--virtual-host` command line option to be more specific of which virtual
host you wish to export.

Use the `apollo-broker store-import` command to import the data.  For example:

    /var/lib/mybroker/bin/apollo-broker store-import myarchive.zip

Just like in the case of the `store-export` command, it will load the
`mybroker`'s configuration and import the archive into the first virtual
host's message store.

## Using the STOMP Protocol

Clients can connect to ${project_name} using the
[STOMP](http://stomp.github.com/) protocol. STOMP provides an interoperable
messaging wire level protocol that allows any STOMP clients can communicate
with any STOMP message broker. It aims to provide easy and widespread
messaging interoperability among many languages, platforms and brokers.

${project_name} supports the following versions of the STOMP specification: 

* [STOMP 1.0](http://stomp.github.com/stomp-specification-1.0.html)
* [STOMP 1.1](http://stomp.github.com/stomp-specification-1.1.html)

The specification is short and simple to read, it is highly recommend that users
to get familiar with it before using one of the many available client libraries.

### Stomp Protocol Options

You can use the `stomp` configuration element within the `connector` element
in the `apollo.xml` configuration file to change the default settings used
in the STOMP protocol implementation.  The `stomp` element supports the 
following configuration attributes:

* `add_user_header` :  Name of the header which will be added to every received 
  message received.  The value of the header will be set to the id of user that 
  sent the message.  Not set by default.
* `add_timestamp_header` :  Name of the header which will be added to every received 
  message received.  The value of the header will be set to the time the message
  was received.  The time will be represented as the number of milliseconds elapsed
  since the UNIX epoch in GMT.  Not set by default.
* `add_redeliveries_header` :  Name of the header which will be added to messages
  sent to consumers if the messages has been redelivered.  The value of the header 
  will be set to the number of times the message has been redeliverd.  Not set 
  by default.
* `max_header_length` : The maximum allowed length of a STOMP header. Defaults 
  to 10240 (10k).
* `max_headers` : The maximum number of allowed headers in a frame.  Defaults 
  to 1000.
* `max_data_length` : The maximum size of the body portion of a STOMP frame.  
  Defaults to 104857600 (100 megs).

The stomp configuration element can also be used to control how the destination 
headers are parsed and interpreted.  The supported attributes are:
  
* `queue_prefix` : Defaults to `/queue/`
* `topic_prefix` : Defaults to `/topic/`
* `path_separator` : Defaults to `.`
* `destination_separator` : Defaults to `,`
* `any_child_wildcard` : Defaults to `*`
* `regex_wildcard_start` : Defaults to `{`
* `regex_wildcard_end` : Defaults to `}`
* `any_descendant_wildcard` : Defaults to `**`

It also supports nested `add_user_header` elements to more finely control how
user headers are added to received STOMP messages.  The `add_user_header` element
should contain the name of the header to set on the STOMP message.  It also 
supports the following attributes:

* `separator` : If user has multiple principles which match, this separator
  will be used to delimit them in the header.  If not set, then only the first
  matching principle will be set in the header.
* `kind` : The principle kind to look for.  Defaults to `*` (matches all 
  principle kinds)

Example:

{pygmentize:: xml}
<connector id="tcp" bind="tcp://0.0.0.0:61613">
  <stomp max_header_length="10000">
    <add_user_header separator=",">user</add_user_header>
  </stomp>
</connector>
{pygmentize}


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

${project_name} supports three types of destinations, queues, topics, and
durable subscriptions.

The difference between queues and topics is how messages are delivered to
consumers. A queue will load balance it's messages over the connected
subscribers so that only one subscriber gets a message. Topics replicate
every message sent to it to all the connected subscribers.  Queues hold 
on to unconsumed messages even when there are no subscriptions attached,
while a topic will drop messages when there are no connected subscriptions.

A durable subscription allows you to create a subscription against a topic
which will queue messages even after the client disconnects.  Clients
can reconnect and consume the queued message originating from the topic
at a later time.

If you want to send or subscribe to a queue, topic, or durable
subscription the STOMP destination should be prefixed with `/queue/`,
`/topic/` or `/dsub/` respectively.

Example STOMP frame sending to a queue:

    SEND
    destination:/queue/a

    hello queue a
    ^@


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

### Message Expiration

${project_name} supports expiring old messages.  Unconsumed expired messages 
are automatically removed from the queue.  You just need to specify when
the message expires by setting the `expires` message header.  The expiration
time must be specified as the number of milliseconds since the Unix epoch.

Example:

    SEND
    destination:/queue/a
    expires:1308690148000

    this message will expire on Tue Jun 21 17:02:28 EDT 2011
    ^@

### Subscription Flow Control

You can add a `credit` header to the `SUBSCRIBE` frame to control the
flow of messages delivered to the client. Each subscription maintains 2
credit windows which allows you to control the flow of messages either based
on number of messages sent or total number of content bytes delivered to the
client. The content bytes are the body of the frame which is usually also
set on the `content-length` header. If either of the two credit windows have
a positive value, the server will deliver messages to the client.

The `credit` header value is expected to use the
`count[,size]` syntax where:

 * count: initial setting of the credit window tracking the remaining number 
   of messages that can be sent.  Must be specified.
 * size: initial setting of the credit window tracking the remaining number 
   of content bytes that can be sent.  Defaults to the receive buffer size of 
   the TCP socket which is typically 65536 bytes.

If the `credit` header is not specified it has the same effect
as if it had been set to `credit:1,65536`.  This setting allows the broker
to optimally stream many small messages to the client or without overloading
the clients processing buffers.

As messages are sent from the server to the client, the credit windows are
reduced by the corresponding amount. When the client send `ACK` frames to the 
server, the credit windows are incremented by the number of messages and content 
sizes corresponding to the messages that were acknowledged.

Example:

    SUBSCRIBE
    id:mysub
    destination:/queue/foo
    credit:5,0
    
    ^@

The above example would cause the subscription to only receive at most 5
messages if the client is not sending any `ACK` messages back to the server.

If you need to receive more messages without acknowledging them than the
size of the credit window, you should use transactions and transactionally
`ACK` messages as they are received.  A transactional ACK increases the 
credit window without actually consuming the messages.  If the transaction
is aborted subsequent acknowledgements of a previously acknowledged message 
does not increase the credit window.

<!-- The following feature seems complicated and not really that useful.-->
<!-- Commenting out as it may get removed. -->
<!--

If the `auto` option is set to `false`, then the credit windows are only
increased when the server receives `ACK` frames which contain a `credit`
header. The header value is expected to use the `count[,size]` syntax. If
`size` is specified, it defaults to 0.

Example:

    SUBSCRIBE
    id:mysub
    destination:/queue/foo
    credit:1,0,false
    
    ^@
    ACK
    id:mysub
    message-id:id-52321
    credit:1,1204
    
    ^@

You can also use the `ACK` frame to increase the credit windows sizes
without needing to acknowledge as message. To do this, don't include the
`message-id` header in the `ACK` frame. Example:

    ACK
    id:mysub
    credit:3
    
    ^@
-->

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

If the durable subscription already exists you can address it directly 
using `/dsub/` prefix on the `destination` header.  For example,
send a message to the previously created `mysub` durable subscription,
you send the following STOMP frame:


    SEND
    destination:/dsub/mysub

    hello durable sub!
    ^@

Similarly, you can also subscribe to the subscription in the same way:

    SUBSCRIBE
    id:0
    destination:/dsub/mysub
    
    ^@

Unlike typical STOMP subscriptions id's which are local to the STOMP 
client's connection, the durable subscription id's are global across
a virtual host.  If two different connections use the same durable
subscription id, then messages from the subscription will get load
balanced across the two connections.  If the second connection uses
a different `destination` or `selector` header, then updates 
the original subscription, and the original connection will
subsequently only receive messages matching the updated
destination or selector.

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

### Queue Auto Delete on `UNSUBSCRIBE`

When you subscribe to a queue, you can add the `auto-delete:true` header
to have the queue automatically deleted when the subscription ends. This
is typically used when implementing the request/reply messaging pattern.
The requesting client creates a temporary dynamically named queue which it
will use to receive responses for requests which sends out. Once the
client unsubscribes or his connection terminates, then the queue is
automatically deleted.

Example:

    SUBSCRIBE
    id:mysub
    destination:/queue/temp.myclientid.1308690148000
    auto-delete:true
    exclusive:true
    
    ^@

If the client does not have sufficient authority to delete the queue, when
the subscription ends, then the STOMP connection will be terminated with
an error message.

The auto-delete feature does not work with composite or wildcard destinations.
it also does not work with topics or durable subscriptions.

### Destination Wildcards

We support destination wildcards to easily subscribe to multiple destinations
with one subscription. This concept has been popular in financial market data
for some time as a way of organizing events (such as price changes) into
hierarchies and to use wildcards for easy subscription of the range of
information you're interested in.

* `.` is used to separate names in a path
* `*` is used to match any name in a path
* `{regex}` is used to match a path name against a regular expression.
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
* `/topic/PRICE.STOCK.*.I*` : Any stock price starting with 'I' on any exchange
* `/topic/PRICE.STOCK.*.*{[0-9]}` : Any stock price that ends in a digit on any exchange

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

### Destination Name Restrictions

Destination names are restricted to using the characters `a-z`, `A-Z`, `0-9`,
`_`, `-` `%`, `~`, or `.` in addition to composite separator `,` and the wild
card `*`.



