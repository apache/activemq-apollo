## Overview

This module implements the AMQP protocol.

## Enabling the SwiftMQ Client Interop Tests

You can use the SwiftMQ AMQP client library to test AMQP 1.0
interoperability with Apollo if you have the clients installed
on your local machine.  To let the Maven build know that the SwiftMQ
clients are available on your machine you need to update
your `~/.m2/settings.xml` file by adding:

      <activeProfiles>
        ...
        <activeProfile>swiftmq-client</activeProfile>
        ..
      </activeProfiles>
      
      <profiles>
        ...
        <profile>
          <id>swiftmq-client</id>
          <properties>
            <swiftmq-client-home>/path/to/swiftmq_9_2_0_client</swiftmq-client-home>
          </properties>
        </profile>
        ...
      </profiles>

Make sure you set the `swiftmq-client-home` to where you installed that clients on your
system.