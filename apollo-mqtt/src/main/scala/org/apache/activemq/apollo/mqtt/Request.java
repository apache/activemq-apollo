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
package org.apache.activemq.apollo.mqtt;

import org.apache.activemq.apollo.broker.DeliveryResult;
import org.apache.activemq.apollo.util.UnitFn1;
import org.fusesource.mqtt.codec.MQTTFrame;
import org.fusesource.mqtt.codec.MessageSupport;

/**
 */
public class Request {

    public final short id;
    public final MessageSupport.Message message;
    public final UnitFn1<DeliveryResult> ack;

    MQTTFrame frame;
    boolean delivered;

    public Request(short id, MessageSupport.Message message, UnitFn1<DeliveryResult> ack) {
        this.id = id;
        this.message = message;
        this.ack = ack;
        frame = message==null ? null : message.encode();
    }
}