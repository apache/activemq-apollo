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
package org.apache.activemq.broker.openwire;

import org.apache.activemq.broker.Destination;
import org.apache.activemq.broker.MessageDelivery;
import org.apache.activemq.command.Message;
import org.apache.activemq.protobuf.AsciiBuffer;

public class OpenWireMessageDelivery implements MessageDelivery {

    private final Message message;
    private Destination destination;
    private AsciiBuffer producerId;
    private Runnable completionCallback;

    public OpenWireMessageDelivery(Message message) {
        this.message = message;
    }

    public Destination getDestination() {
        if( destination == null ) {
            destination = OpenwireProtocolHandler.convert(message.getDestination());
        }
        return destination;
    }

    public int getFlowLimiterSize() {
        return message.getSize();
    }

    public int getPriority() {
        return message.getPriority();
    }

    public AsciiBuffer getMsgId() {
        return null;
    }

    public AsciiBuffer getProducerId() {
        if( producerId == null ) {
            producerId = new AsciiBuffer(message.getProducerId().toString());
        }
        return producerId;
    }

    public Message getMessage() {
        return message;
    }

    public Runnable getCompletionCallback() {
        return completionCallback;
    }

    public void setCompletionCallback(Runnable completionCallback) {
        this.completionCallback = completionCallback;
    }

    public <T> T asType(Class<T> type) {
        if( type == Message.class ) {
            return type.cast(message);
        }
        // TODO: is this right?
        if( message.getClass().isAssignableFrom(type) ) {
            return type.cast(message);
        }
        return null;
    }

}
