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
package org.apache.activemq.flow;

import java.io.Serializable;
import java.util.HashSet;

import org.apache.activemq.flow.Flow;
import org.apache.activemq.queue.Mapper;

public class Message implements Serializable {

    public static final Mapper<Integer, Message> PRIORITY_MAPPER = new Mapper<Integer, Message>() {
        public Integer map(Message element) {
            return element.priority;
        }
    };

    public static final int MAX_USER_PRIORITY = 10;
    public static final int MAX_PRIORITY = MAX_USER_PRIORITY + 1;
    public static final int SYSTEM_PRIORITY = MAX_PRIORITY;

    public static final short TYPE_NORMAL = 0;
    public static final short TYPE_FLOW_CONTROL = 1;
    public static final short TYPE_FLOW_OPEN = 2;
    public static final short TYPE_FLOW_CLOSE = 3;

    final String msg;
    transient final Flow flow;
    final Destination dest;
    int hopCount;
    HashSet<String> matchProps;
    final long msgId;
    final int producerId;
    final int priority;

    Message(long msgId, int producerId, String msg, Flow flow, Destination dest, int priority) {
        this.msgId = msgId;
        this.producerId = producerId;
        this.msg = msg;
        this.flow = flow;
        this.dest = dest;
        this.priority = priority;
        hopCount = 0;
    }

    Message(Message m) {
        this.msgId = m.msgId;
        this.producerId = m.producerId;
        this.msg = m.msg;
        this.flow = m.flow;
        this.dest = m.dest;
        this.matchProps = m.matchProps;
        this.priority = m.priority;
        hopCount = m.hopCount;
    }

    public short type() {
        return TYPE_NORMAL;
    }

    public void setProperty(String matchProp) {
        if (matchProps == null) {
            matchProps = new HashSet<String>();
        }
        matchProps.add(matchProp);
    }

    public boolean match(String matchProp) {
        if (matchProps == null) {
            return false;
        }
        return matchProps.contains(matchProp);
    }

    public boolean isSystem() {
        return false;
    }

    public void incrementHopCount() {
        hopCount++;
    }

    public final int getHopCount() {
        return hopCount;
    }

    public final Destination getDestination() {
        return dest;
    }

    public Flow getFlow() {
        return flow;
    }

    public int getFlowLimiterSize() {
        return 1;
    }

    public int getPriority() {
        return priority;
    }

    public String toString() {
        return "Message: " + msg + " flow + " + flow + " dest: " + dest;
    }

    public long getMsgId() {
        return msgId;
    }

    public int getProducerId() {
        return producerId;
    }
}
