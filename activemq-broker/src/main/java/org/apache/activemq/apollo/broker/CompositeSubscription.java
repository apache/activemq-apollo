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
package org.apache.activemq.apollo.broker;

import java.util.ArrayList;

import org.apache.activemq.apollo.broker.ProtocolHandler.ConsumerContext;

/**
 * CompositeSubscription
 * <p>
 * Description:
 * </p>
 * 
 * @author cmacnaug
 * @version 1.0
 */
public class CompositeSubscription implements BrokerSubscription {

    private final Destination destination;
    
    private final ArrayList<BrokerSubscription> subscriptions;

    public CompositeSubscription(Destination destination, ArrayList<BrokerSubscription> subscriptions) {
        this.destination = destination;
        this.subscriptions = subscriptions;
    }

    public void connect(ConsumerContext consumer) throws Exception {
        for (BrokerSubscription sub : subscriptions) {
            sub.connect(consumer);
        }
    }

    public synchronized void disconnect(ConsumerContext consumer) {
        for (BrokerSubscription sub : subscriptions) {
            sub.disconnect(consumer);
        }
    }

    public Destination getDestination() {
        return destination;
    }

}
