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
package org.apache.activemq.queue.actor.perf;

import java.io.Serializable;

import org.apache.activemq.flow.Flow;
import org.apache.activemq.flow.Commands.Destination;
import org.apache.activemq.flow.Commands.Message.MessageBean;
import org.apache.activemq.flow.Commands.Message.MessageBuffer;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.Mapper;
import org.fusesource.hawtbuf.UTF8Buffer;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class Message implements Serializable {

    private static final long serialVersionUID = 6759761889075451996L;

    public static final Mapper<Integer, Message> PRIORITY_MAPPER = new Mapper<Integer, Message>() {
        public Integer map(Message element) {
            return element.getPriority();
        }
    };

    public static final int MAX_USER_PRIORITY = 10;
    public static final int MAX_PRIORITY = MAX_USER_PRIORITY + 1;
    public static final int SYSTEM_PRIORITY = MAX_PRIORITY;

    public static final short TYPE_NORMAL = 0;
    public static final short TYPE_FLOW_CONTROL = 1;
    public static final short TYPE_FLOW_OPEN = 2;
    public static final short TYPE_FLOW_CLOSE = 3;

    transient Flow flow;
    private MessageBuffer message;

    Message(long msgId, int producerId, String msg, Flow flow, Destination dest, int priority) {
        MessageBean message = new MessageBean();
        message.setMsgId(msgId);
        message.setProducerId(producerId);
        message.setMsg(new UTF8Buffer(msg));
        message.setDest(dest);
        message.setPriority(priority);
        this.message = message.freeze();
        this.flow = flow;
    }

    Message(Message m) {
        this.message = m.message;
        this.flow = m.flow;
    }

    public Message(MessageBuffer m) {
        this.message=m;
    }

    public short type() {
        return TYPE_NORMAL;
    }

    public void setProperty(String matchProp) {
        message = message.copy().addProperty(matchProp).freeze();
    }

    public boolean match(String matchProp) {
        if (!message.hasProperty()) {
            return false;
        }
        return message.getPropertyList().contains(matchProp);
    }

    public boolean isSystem() {
        return false;
    }

    public void incrementHopCount() {
        message = message.copy().setHopCount(message.getHopCount()).freeze();
    }

    public final int getHopCount() {
        return message.getHopCount();
    }

    public final Destination getDestination() {
        return message.getDest();
    }

    public Flow getFlow() {
        return flow;
    }

    public int getFlowLimiterSize() {
        return 1;
    }

    public int getPriority() {
        return message.getPriority();
    }

    public String toString() {
        return IntrospectionSupport.toString(this);
    }

    public long getMsgId() {
        return message.getMsgId();
    }

    public int getProducerId() {
        return message.getProducerId();
    }

    public MessageBuffer getProto() {
        return message;
    }
}
