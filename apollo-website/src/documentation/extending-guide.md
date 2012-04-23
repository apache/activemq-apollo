# Apollo ${project_version} Extending Guide

{:toc:2-5}

## Overview

Apollo support being extended in several ways.  This guide documents 
all the supported extension points.

### Adding your Extensions to the Apollo Class Path

Just create a `.jar` out of out code and add it to the `${apollo.home}/lib`
directory.  When Apollo restarts it will add the new jar to it's class path.

### Extension Discovery

Apollo discovers your extensions by looking for extension resource files in
all the jar files in it's classpath.  For example, the `apollo-broker` jar
file contains the following a resource file `META-INF/services/org.apache.activemq.apollo/protocol-codec-factory.index`
It contains the class names of the protocol codec factories that are implemented
in the `broker-core` module.  It's contents are:

    org.apache.activemq.apollo.broker.protocol.AnyProtocolFactory
    org.apache.activemq.apollo.broker.protocol.UdpProtocolFactory
    org.apache.activemq.apollo.broker.protocol.RawProtocolFactory

All three of the the listed classes implement the `ProtocolFactory` interface.
Here's a list of extension points supported by Apollo:

* Data Model: `META-INF/services/org.apache.activemq.apollo/dto-module.index` ->
  `org.apache.activemq.apollo.util.DtoModule`

* Transports: `META-INF/services/org.apache.activemq.apollo/transport-factory.index` ->
  `org.apache.activemq.apollo.broker.transport.TransportFactory.Provider`

* Protocols: `META-INF/services/org.apache.activemq.apollo/protocol-factory.index` ->
  `org.apache.activemq.apollo.broker.protocol.ProtocolFactory`

* Broker Factories: `META-INF/services/org.apache.activemq.apollo/broker-factory.index` ->
  `org.apache.activemq.apollo.broker.BrokerFactoryTrait`

* Connectors: `META-INF/services/org.apache.activemq.apollo/connector-factory.index` ->
  `org.apache.activemq.apollo.broker.ConnectorFactory`

* Virtual Hosts: `META-INF/services/org.apache.activemq.apollo/virtual-host-factory.index` ->
  `org.apache.activemq.apollo.broker.VirtualHostFactory`

* Route Listeners: `META-INF/services/org.apache.activemq.apollo/router-listener-factory.index` ->
  `org.apache.activemq.apollo.broker.RouterListenerFactory`

* Stores: `META-INF/services/org.apache.activemq.apollo/store-factory.index` ->
  `org.apache.activemq.apollo.broker.store.StoreFactory`

* Web Admin Components: `META-INF/services/org.apache.activemq.apollo/web-module.index` ->
  `org.apache.activemq.apollo.web.WebModule`

* Web Servers -> `META-INF/services/org.apache.activemq.apollo/web-server-factory.index`->
  `org.apache.activemq.apollo.broker.web.WebServerFactory`

<!-- These might go away...
* `META-INF/services/org.apache.activemq.apollo/binding-factory.index`:
  `org.apache.activemq.apollo.broker.BindingFactory` 
* `META-INF/services/org.apache.activemq.apollo/protocol-codec-factory.index` : 
  `org.apache.activemq.apollo.broker.protocol.ProtocolCodecFactory.Provider`
-->

### Extending the Data/Configuraiton model with new Objects

If you want to extend the Apollo xml configuration model to understand some
custom JAXB object you have defined in your own packages, then you need
to implement a `Module` class and then create a `META-INF/services/org.apache.activemq.apollo/dto-module.index` 
resource file in which you list it's class name.

Example module class:

{pygmentize:: scala}
package org.example
import org.apache.activemq.apollo.util.DtoModule

class Module extends DtoModule {

  def dto_package = "org.apache.activemq.apollo.broker.store.leveldb.dto"
  def extension_classes = Array(classOf[LevelDBStoreDTO], classOf[LevelDBStoreStatusDTO])

}
{pygmentize}

Example `META-INF/services/org.apache.activemq.apollo/dto-module.index` resource:

    org.example.Module

### Plugging into the Broker Lifecycle

You can implement custom [Service][] objects which get started / stopped when 
the broker starts and stops.  Once you have packaged your custom
service, and added it to the Apollo class path, you can 
update the `apollo.xml` to add the service so it gets started when 
apollo starts:

{pygmentize:: xml}
<service id='myservice' kind='org.example.MyService'/>
{pygmentize}

The `id` attribute is a unique service name of your service, and the 
`kind` attribute is the class name of your service. 

If your service needs a reference to the Broker object which is running
in, add the following field definition to your class:

{pygmentize:: scala}
var broker:Broker = null
{pygmentize}

The broker instance will be injected into your class instance before it gets 
started.

Your service can also get reference to to the configuration element used
to define it if it defines the following field.

{pygmentize:: scala}
var config: CustomServiceDTO
{pygmentize}

This field will also get injected before getting started.  The `CustomServiceDTO.other`
field will contain any additional configuration elements defined within service
element.  For example, if you configured the service as follows:

{pygmentize:: xml}
<service id='myservice' kind='org.example.MyService'/>
  <options xmlns="http://example.org/myservice">
    <search>google.com</search>
  </options>
</service>
{pygmentize}

Then you could access the options DOM element using:

{pygmentize:: scala}
    val options = config.other.get(1).asInstanceOf[Element]
{pygmentize}

If you had defined JAXB object mappings for the `<options>` class
then `config` will hold that object instead of generic
DOM `Element`.
