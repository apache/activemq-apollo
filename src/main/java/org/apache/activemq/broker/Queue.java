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

import java.util.HashMap;

import org.apache.activemq.broker.DeliveryTarget;
import org.apache.activemq.broker.Destination;
import org.apache.activemq.broker.MessageDelivery;
import org.apache.activemq.flow.IFlowSink;
import org.apache.activemq.flow.ISourceController;
import org.apache.activemq.flow.PrioritySizeLimiter;
import org.apache.activemq.flow.SizeLimiter;
import org.apache.activemq.protobuf.AsciiBuffer;
import org.apache.activemq.queue.IQueue;
import org.apache.activemq.queue.Mapper;
import org.apache.activemq.queue.PartitionedQueue;
import org.apache.activemq.queue.SharedPriorityQueue;
import org.apache.activemq.queue.SharedQueue;
import org.apache.activemq.queue.Subscription;

public class Queue implements DeliveryTarget {

    HashMap<DeliveryTarget, Subscription<MessageDelivery>> subs = new HashMap<DeliveryTarget, Subscription<MessageDelivery>>();
    private Destination destination;
    private IQueue<AsciiBuffer, MessageDelivery> queue;
    private MessageBroker broker;
    
    private Mapper<Integer, MessageDelivery> partitionMapper;
    private Mapper<AsciiBuffer, MessageDelivery> keyExtractor;

    private IQueue<AsciiBuffer, MessageDelivery> createQueue() {

        if (partitionMapper!=null) {
            PartitionedQueue<Integer, AsciiBuffer, MessageDelivery> queue = new PartitionedQueue<Integer, AsciiBuffer, MessageDelivery>() {
                @Override
                protected IQueue<AsciiBuffer, MessageDelivery> cratePartition(Integer partitionKey) {
                    return createSharedFlowQueue();
                }
            };
            queue.setPartitionMapper(partitionMapper);
            queue.setResourceName(destination.getName().toString());
            return queue;
        } else {
            return createSharedFlowQueue();
        }
    }


    public static final Mapper<Integer, MessageDelivery> PRIORITY_MAPPER = new Mapper<Integer, MessageDelivery>() {
        public Integer map(MessageDelivery element) {
            return element.getPriority();
        }
    };
    
    private IQueue<AsciiBuffer, MessageDelivery> createSharedFlowQueue() {
        if (MessageBroker.MAX_PRIORITY > 1) {
            PrioritySizeLimiter<MessageDelivery> limiter = new PrioritySizeLimiter<MessageDelivery>(100, 1, MessageBroker.MAX_PRIORITY);
            limiter.setPriorityMapper(PRIORITY_MAPPER);
            SharedPriorityQueue<AsciiBuffer, MessageDelivery> queue = new SharedPriorityQueue<AsciiBuffer, MessageDelivery>(destination.getName().toString(), limiter);
            queue.setKeyMapper(keyExtractor);
            queue.setAutoRelease(true);
            queue.setDispatcher(broker.getDispatcher());
            return queue;
        } else {
            SizeLimiter<MessageDelivery> limiter = new SizeLimiter<MessageDelivery>(100, 1);
            SharedQueue<AsciiBuffer, MessageDelivery> queue = new SharedQueue<AsciiBuffer, MessageDelivery>(destination.getName().toString(), limiter);
            queue.setKeyMapper(keyExtractor);
            queue.setAutoRelease(true);
            queue.setDispatcher(broker.getDispatcher());
            return queue;
        }
    }

    public final void deliver(ISourceController<MessageDelivery> source, MessageDelivery msg) {
        queue.add(msg, source);
    }
    
    public final Destination getDestination() {
        return destination;
    }

    public final void addConsumer(final DeliveryTarget dt) {
        Subscription<MessageDelivery> sub = new Subscription<MessageDelivery>() {
            public boolean isPreAcquired() {
                return true;
            }

            public boolean matches(MessageDelivery message) {
                return dt.match(message);
            }

            public boolean isRemoveOnDispatch() {
                return true;
            }

            public IFlowSink<MessageDelivery> getSink() {
                return dt.getSink();
            }

            @Override
            public String toString() {
                return getSink().toString();
            }
        };
        subs.put(dt, sub);
        queue.addSubscription(sub);
    }

    public boolean removeSubscirption(final DeliveryTarget dt) {
        Subscription<MessageDelivery> sub = subs.remove(dt);
        if (sub != null) {
            return queue.removeSubscription(sub);
        }
        return false;
    }

    public void start() throws Exception {
        queue = createQueue();
    }

    public void stop() throws Exception {
    }

    public IFlowSink<MessageDelivery> getSink() {
        return queue;
    }

    public boolean match(MessageDelivery message) {
        return true;
    }

    public MessageBroker getBroker() {
        return broker;
    }

    public void setBroker(MessageBroker broker) {
        this.broker = broker;
    }

    public Mapper<Integer, MessageDelivery> getPartitionMapper() {
        return partitionMapper;
    }

    public void setPartitionMapper(Mapper<Integer, MessageDelivery> partitionMapper) {
        this.partitionMapper = partitionMapper;
    }

    public Mapper<AsciiBuffer, MessageDelivery> getKeyExtractor() {
        return keyExtractor;
    }

    public void setKeyExtractor(Mapper<AsciiBuffer, MessageDelivery> keyExtractor) {
        this.keyExtractor = keyExtractor;
    }

    public void setDestination(Destination destination) {
        this.destination = destination;
    }

    public AsciiBuffer getPersistentQueueName() {
        // TODO Auto-generated method stub
        return destination.getName();
    }

    public boolean isDurable() {
        return true;
    }

}