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
# Getting Started Guide

{:toc:2-5}

This guide will help you install, setup and run an Apollo broker and validate
that the broker is operating correctly.

## Installation

1. [Download](../download.html) the ${project_name} distribution that is 
   most appropriate for your operating system.

2. Extract the distribution archive:
   {pygmentize_and_compare::wide=true}
-----------------------------
text: Unix/Linux/OS X
-----------------------------
tar -zxvf apache-apollo-${project_version}-unix-distro.tar.gz
-----------------------------
text: Windows
-----------------------------
jar -xvf apache-apollo-${project_version}-windows-distro.zip
{pygmentize_and_compare}

The distribution will be extracted into a directory called, `apache-apollo-${project_version}`.
The rest of dis document will refer the full path to this directory as `${APOLLO_HOME}`.

### Creating a Broker Instance

A broker instance is the directory containing all the configuration and runtime
data such as logs and data files associated with a broker process.  It is recommended that
you do *not* create the instance directory under `${APOLLO_HOME}`.  This separation is
encouraged so that you can more easily upgrade when the next version of Apollo is released.

On unix systems, it is a common convention to store this kind of runtime data under 
the `/var/lib` directory.  For example, to create an instance at '/var/lib/mybroker', run:

    cd /var/lib
    ${APOLLO_HOME}/bin/apollo create mybroker

A broker instance directory will contain the following sub directories:

 * `bin`: holds execution scripts associated with this instance.
 * `etc`; hold the instance configuration files
 * `data`: holds the data files used for storing persistent messages
 * `log`: holds rotating log files
 * `tmp`: holds temporary files that are safe to delete between broker runs

At this point you may want to adjust the default configuration located in
the `etc` directory.


### Running a Broker Instance

Assuming you created the broker instance under `/var/lib/mybroker` all you need
to do start running the broker instance is execute:

    /var/lib/mybroker/bin/apollo-broker run

Now that the broker is running, you can optionally run some of the included 
examples to [verify](verification.html) the the broker is running properly.

### Web Administration

Apollo provides a simple web interface to monitor the status of the broker.  Once
the admin interface will be accessible at:

* [http://127.0.0.1:61680/](http://127.0.0.1:61680/) or [https://127.0.0.1:61681/](https://127.0.0.1:61681/)

The default login id and password is `admin` and `password`.

## Installing Recommended Optional Features

### Switching to the BDB Store

Apollo's fastest message store implementation is the BDB based message store.  
Unfortunately, BDB cannot be redistributed by Apache.  It is highly recommended
that you add it to your apollo installation. You can download it from Oracle at
[je-4.1.10.jar](http://download.oracle.com/maven/com/sleepycat/je/4.1.10/je-4.1.10.jar) and
then copy it into the `${APOLLO_HOME}/lib` directory.

For those of you with curl installed, you can just run:

    curl http://download.oracle.com/maven/com/sleepycat/je/4.1.10/je-4.1.10.jar > ${APOLLO_HOME}/lib/je-4.1.10.jar
    
Once installed, you then need to update the configuration by editing the `etc/apollo.xml` file and
replace `jdbm2_store` with `bdb_store`.
