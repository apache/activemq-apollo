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
package org.apache.activemq.broker;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

import org.apache.activemq.broker.DeliveryTarget;
import org.apache.activemq.broker.Domain;
import org.apache.activemq.broker.MessageDelivery;
import org.apache.activemq.broker.Queue;
import org.apache.activemq.protobuf.AsciiBuffer;

public class QueueDomain implements Domain {
    
    final HashMap<AsciiBuffer, Queue> queues = new HashMap<AsciiBuffer, Queue>();

    public void add(AsciiBuffer name, Object queue) {
        queues.put(name, (Queue)queue);
    }
    public Object remove(AsciiBuffer name) {
        return queues.remove(name);
    }

    public void bind(AsciiBuffer name, DeliveryTarget deliveryTarget) {
        queues.get(name).addConsumer(deliveryTarget);
    }

    public Collection<DeliveryTarget> route(AsciiBuffer name, MessageDelivery delivery) {
        Queue queue = queues.get(name);
        if( queue!=null ) {
            ArrayList<DeliveryTarget> rc = new ArrayList<DeliveryTarget>(1);
            rc.add(queue);
            return rc;
        }
        return null;
    }

}
