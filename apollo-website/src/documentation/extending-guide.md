# Apollo ${project_version} Extending Guide

{:toc:2-5}

## Overview

Apollo support being extended in several ways.  This guide documents 
all the supported extension points.

### Adding your Extensions to the Apollo Class Path

Just create a `.jar` out of out code and add it to the `${apollo.home}/lib`
directory.  When Apollo restarts it will add the new jar to it's class path.

### Extending the JAXB model with new Objects

If you want to extend the Apollo xml configuration model to understand some
custom JAXB object you have defined in your own packages, then you need
to implement a `Module` class and then create a `META-INF/services/org.apache.activemq.apollo/modules.index` 
resource file in which you list it's class name.

Example module class:

{pygmentize:: scala}
package org.example
import org.apache.activemq.apollo.util.JaxbModule

class ExtensionJaxbModule extends JaxbModule {
  def xml_package = "org.example.dto"
}
{pygmentize}


Example `META-INF/services/org.apache.activemq.apollo/jaxb-module.index` resource:

    org.example.ExtensionJaxbModule

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
