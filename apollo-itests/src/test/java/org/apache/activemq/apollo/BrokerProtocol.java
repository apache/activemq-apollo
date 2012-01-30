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
package org.apache.activemq.apollo;

import org.apache.activemq.apollo.broker.Broker;
import org.apache.activemq.apollo.broker.BrokerFactory;
import org.apache.activemq.apollo.util.ServiceControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.ConnectionFactory;
import java.net.InetSocketAddress;

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
abstract public class BrokerProtocol {
    protected static final Logger LOG = LoggerFactory.getLogger(BrokerProtocol.class);

    public Object create(String config) {
        LOG.info("Loading broker configuration from the classpath with URI: " + config);
        return BrokerFactory.createBroker(config);
    }
    public void start(Object broker) {
        ServiceControl.start((Broker)broker, "Starting "+broker);
    }
    public void stop(Object broker) {
        ServiceControl.stop((Broker)broker, "Stopping "+broker);
    }

    public int port(Object broker) {
        Broker b = (Broker) broker;
        InetSocketAddress address = (InetSocketAddress) b.get_socket_address();
        return address.getPort();
    }

    abstract ConnectionFactory getConnectionFactory(Object broker);

}
