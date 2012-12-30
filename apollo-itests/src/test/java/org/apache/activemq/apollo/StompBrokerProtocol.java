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

import org.apache.activemq.command.ActiveMQDestination;
import org.fusesource.stomp.jms.*;

import javax.jms.*;

import java.util.HashMap;

import static java.lang.String.format;

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class StompBrokerProtocol extends BrokerProtocol {

    @Override
    public ConnectionFactory getConnectionFactory(Object broker) {
        StompJmsConnectionFactory factory = new StompJmsConnectionFactory();
        factory.setBrokerURI(format("tcp://localhost:%s", port(broker)));
        return factory;
    }

    @Override
    public String toString() {
        return "STOMP";
    }

    @Override
    public String name(Destination destination) {
        return ((StompJmsDestination)destination).getName();
    }

    @Override
    public Queue createQueue(String name) {
        return new StompJmsQueue("/queue/", name);
    }

    @Override
    public Topic createTopic(String name) {
        return new StompJmsTopic("/topic/", name);
    }

    @Override
    public void setPrefetch(Connection connection, int value) {
        ((StompJmsConnection)connection).setPrefetch(new StompJmsPrefetch(value));
    }

    @Override
    public Destination addExclusiveOptions(Destination dest) {
        HashMap<String, String> headers = new HashMap<String, String>();
        headers.put("exclusive", "true");
        final StompJmsQueue copy = ((StompJmsQueue) dest).copy();
        copy.setSubscribeHeaders(headers);
        return copy;
    }

}
