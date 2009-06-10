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

import org.apache.activemq.apollo.broker.DeliveryTarget;
import org.apache.activemq.apollo.broker.MessageDelivery;
import org.apache.activemq.protobuf.AsciiBuffer;

/**
 * Represents a messaging domain like pub/sub or point to point in JMS terms or an Exchange in
 * AMQP terms.
 * 
 * @author chirino
 */
public interface Domain {

    public void add(AsciiBuffer destinationName, Object destination);
    
    public Object remove(AsciiBuffer destinationName);

    public void bind(AsciiBuffer destinationName, DeliveryTarget target);
    
    public void unbind(AsciiBuffer destinationName, DeliveryTarget target);
    
    public Collection<DeliveryTarget> route(AsciiBuffer destinationName, MessageDelivery message);
    
}
