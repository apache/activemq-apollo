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

import java.util.Collection;

import org.apache.activemq.apollo.broker.path.PathMap;
import org.apache.activemq.util.buffer.AsciiBuffer;

public class Domain {

    private final PathMap<DeliveryTarget> targets = new PathMap<DeliveryTarget>();

    synchronized public void bind(AsciiBuffer name, DeliveryTarget queue) {
        targets.put(name, queue);
    }
    
    synchronized public void unbind(AsciiBuffer name, DeliveryTarget queue) {
        targets.remove(name, queue);
    }

    synchronized public Collection<DeliveryTarget> route(AsciiBuffer name, MessageDelivery delivery) {
        return targets.get(name);
    }

}
