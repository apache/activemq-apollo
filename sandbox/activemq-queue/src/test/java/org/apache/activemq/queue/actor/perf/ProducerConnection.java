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

import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.actor.ActorProxy;
import org.apache.activemq.flow.Commands.Destination;
import org.apache.activemq.metric.MetricAggregator;
import org.apache.activemq.apollo.util.metric.MetricCounter;

import static java.util.concurrent.TimeUnit.*;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class ProducerConnection extends ClientConnection {
    
    private int priority;
    private int priorityMod;
    private int producerId;
    private Destination destination;
    private String property;
    private MetricAggregator totalProducerRate;
    private int payloadSize = 0;
    private final MetricCounter rate = new MetricCounter();
    private AtomicLong messageIdGenerator;
    volatile private long thinkTime;

    protected void createActor() {
        actor = ActorProxy.create(ConnectionStateActor.class, new ProducerConnectionState(), dispatchQueue);
    }
    
    private static final long MATCH_WINDOW = MILLISECONDS.toNanos(100);

    class ProducerConnectionState extends ClientConnectionState {
        
        private String filler;
        private int payloadCounter;
        private boolean stopped;
        private boolean schedualed;

        @Override
        public void onStart() {
            rate.name("Producer " + name + " Rate");
            totalProducerRate.add(rate);

            if (payloadSize > 0) {
                StringBuilder sb = new StringBuilder(payloadSize);
                for (int i = 0; i < payloadSize; ++i) {
                    sb.append((char) ('a' + (i % 26)));
                }
                filler = sb.toString();
            }
            super.onStart();
        }
        
        @Override
        public void onConnect() {
            super.onConnect();
            produceMessages();
        }

        protected void onSessionResume() {
            produceMessages();
        }
        
        @Override
        public void onStop() {
            stopped = true;
            super.onStop();
        }
        
        
        private void produceMessages() {
            while( !isSessionSendBlocked() && !stopped && !schedualed ) {
                sendMessage();
                if( thinkTime > 0 ) {
                    schedualNextSend();
                    return;
                }
            }
        }

        private void schedualNextSend() {
            schedualed=true;
            dispatchQueue.dispatchAfter(new Runnable() {
                public void run() {
                   schedualed = false;
                   produceMessages(); 
                }
            }, thinkTime, MILLISECONDS);
        }

        private void sendMessage() {
            int p = priority;
            if (priorityMod > 0) {
                p = payloadCounter % priorityMod == 0 ? 0 : p;
            }

            Message next = new Message(messageIdGenerator.incrementAndGet(), producerId, createPayload(), null, destination, p);
            if (property != null) {
                next.setProperty(property);
            }
            sessionSend(next);
            rate.increment();
        }
        
        private String createPayload() {
            if (payloadSize >= 0) {
                StringBuilder sb = new StringBuilder(payloadSize);
                sb.append(name);
                sb.append(':');
                sb.append(++payloadCounter);
                sb.append(':');
                int length = sb.length();
                if (length <= payloadSize) {
                    sb.append(filler.subSequence(0, payloadSize - length));
                    return sb.toString();
                } else {
                    return sb.substring(0, payloadSize);
                }
            } else {
                return name + ":" + (++payloadCounter);
            }
        }
        
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public int getPriorityMod() {
        return priorityMod;
    }

    public void setPriorityMod(int priorityMod) {
        this.priorityMod = priorityMod;
    }

    public int getProducerId() {
        return producerId;
    }

    public void setProducerId(int producerId) {
        this.producerId = producerId;
    }

    public Destination getDestination() {
        return destination;
    }

    public void setDestination(Destination destination) {
        this.destination = destination;
    }

    public String getProperty() {
        return property;
    }

    public void setProperty(String property) {
        this.property = property;
    }

    public MetricAggregator getTotalProducerRate() {
        return totalProducerRate;
    }

    public void setTotalProducerRate(MetricAggregator totalProducerRate) {
        this.totalProducerRate = totalProducerRate;
    }

    public int getPayloadSize() {
        return payloadSize;
    }

    public void setPayloadSize(int payloadSize) {
        this.payloadSize = payloadSize;
    }

    public MetricCounter getRate() {
        return rate;
    }

    public AtomicLong getMessageIdGenerator() {
        return messageIdGenerator;
    }

    public void setMessageIdGenerator(AtomicLong messageIdGenerator) {
        this.messageIdGenerator = messageIdGenerator;
    }

    public long getThinkTime() {
        return thinkTime;
    }

    public void setThinkTime(long thinkTime) {
        this.thinkTime = thinkTime;
    }
    

}
