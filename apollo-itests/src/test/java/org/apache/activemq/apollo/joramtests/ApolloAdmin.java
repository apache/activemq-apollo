/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.apollo.joramtests;

import org.apache.activemq.apollo.BrokerProtocol;
import org.apache.activemq.apollo.JmsTestBase;
import org.apache.activemq.apollo.broker.Broker;
import org.apache.activemq.apollo.broker.BrokerTestSupport;
import org.objectweb.jtests.jms.admin.Admin;

import javax.jms.ConnectionFactory;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.Hashtable;

/**
 *
 */
abstract public class ApolloAdmin implements Admin {

    static protected JmsTestBase base = new JmsTestBase();

    Context context;
    {
        try {
            context = new InitialContext(new Hashtable<String, String>());
        } catch (NamingException e) {
            throw new RuntimeException(e);
        }
    }

    protected abstract BrokerProtocol createProtocol();

    public String getName() {
        return getClass().getName();
    }

    public void startServer() throws Exception {
        base.protocol = createProtocol();
        base.startBroker();
    }

    public void stopServer() throws Exception {
        base.stopBroker();
    }

    public void start() throws Exception {
    }

    public void stop() throws Exception {
    }

    public Context createContext() throws NamingException {
        return context;
    }

    public void createQueue(String name) {
        try {
            context.bind(name, base.protocol.createQueue(name));
        } catch (NamingException e) {
            throw new RuntimeException(e);
        }
    }

    public void createTopic(String name) {
        try {
            context.bind(name, base.protocol.createTopic(name));
        } catch (NamingException e) {
            throw new RuntimeException(e);
        }
    }

    public void deleteQueue(String name) {
        BrokerTestSupport.delete_queue((Broker)base.broker, name);
        try {
            context.unbind(name);
        } catch (NamingException e) {
            throw new RuntimeException(e);
        }
    }

    public void deleteTopic(String name) {
        try {
            context.unbind(name);
        } catch (NamingException e) {
            throw new RuntimeException(e);
        }
    }

    public void createConnectionFactory(String name) {
        try {
            final ConnectionFactory factory = base.protocol.getConnectionFactory(base.broker);
            context.bind(name, factory);
        } catch (NamingException e) {
            throw new RuntimeException(e);
        }
    }

    public void deleteConnectionFactory(String name) {
        try {
            context.unbind(name);
        } catch (NamingException e) {
            throw new RuntimeException(e);
        }
    }

    public void createQueueConnectionFactory(String name) {
        createConnectionFactory(name);
    }
    public void createTopicConnectionFactory(String name) {
        createConnectionFactory(name);
    }
    public void deleteQueueConnectionFactory(String name) {
        deleteConnectionFactory(name);
    }
    public void deleteTopicConnectionFactory(String name) {
        deleteConnectionFactory(name);
    }

}
