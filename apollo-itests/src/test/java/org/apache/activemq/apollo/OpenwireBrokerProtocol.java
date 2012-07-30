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

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.*;

import static java.lang.String.*;

import javax.jms.*;
import java.util.HashMap;
import java.util.Properties;

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class OpenwireBrokerProtocol extends BrokerProtocol {
    
    @Override
    public ConnectionFactory getConnectionFactory(Object broker) {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory();
        factory.setBrokerURL(format("tcp://localhost:%s", port(broker)));
        factory.setNestedMapAndListEnabled(false);
        return factory;
    }

    @Override
    public String toString() {
        return "OpenWire";
    }

    @Override
    public String name(Destination destination) {
        return ((ActiveMQDestination)destination).getPhysicalName();
    }

    @Override
    public Queue createQueue(String name) {
        return new ActiveMQQueue(name);
    }

    @Override
    public Topic createTopic(String name) {
        return new ActiveMQTopic(name);
    }

    @Override
    public void setPrefetch(Connection connection, int value) {
        ((ActiveMQConnection)connection).getPrefetchPolicy().setAll(value);
    }

    @Override
    public Destination addExclusiveOptions(Destination dest) {
        final HashMap<String, String> o = new HashMap<String, String>();
        o.put("consumer.exclusive", "true");
        if( dest instanceof ActiveMQTempTopic ) {
            return new ActiveMQTopic(((ActiveMQTempTopic)dest).getPhysicalName()) {
                {
                    options = o;
                }
            };
        }
        if( dest instanceof ActiveMQTempQueue ) {
            return new ActiveMQTempQueue(((ActiveMQTempQueue)dest).getPhysicalName()) {
                {
                    options = o;
                }
            };
        }
        if( dest instanceof ActiveMQTopic ) {
            return new ActiveMQTopic(((ActiveMQTopic)dest).getPhysicalName()) {
                {
                    options = o;
                }
            };
        }
        if( dest instanceof ActiveMQQueue ) {
            return new ActiveMQQueue(((ActiveMQQueue)dest).getPhysicalName()) {
                {
                    options = o;
                }
            };
        }
        return dest;
    }

}
