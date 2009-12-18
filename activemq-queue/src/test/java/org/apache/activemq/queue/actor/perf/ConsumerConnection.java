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

import java.util.concurrent.TimeUnit;

import org.apache.activemq.actor.ActorProxy;
import org.apache.activemq.flow.Commands.Destination;
import org.apache.activemq.metric.MetricAggregator;
import org.apache.activemq.metric.MetricCounter;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class ConsumerConnection extends ClientConnection {
    
    private MetricAggregator totalConsumerRate;
    volatile private long thinkTime;
    private Destination destination;
    private String selector;
    private final MetricCounter rate = new MetricCounter();

    protected void createActor() {
        actor = ActorProxy.create(ConnectionStateActor.class, new ConsumerConnectionState(), dispatchQueue);
    }

    class ConsumerConnectionState extends ClientConnectionState {

        @Override
        public void onStart() {
            rate.name("Consumer " + name + " Rate");
            totalConsumerRate.add(rate);
            super.onStart();
        }
        
        @Override
        public void onConnect() {
            super.onConnect();
            transportSend(destination);
        }

        @Override
        protected void onReceiveMessage(final Message msg) {
            if (thinkTime > 0) {
                dispatchQueue.dispatchAfter(new Runnable() {
                    public void run() {
                        rate.increment();
                        ConsumerConnectionState.super.onReceiveMessage(msg);
                    }
                }, thinkTime, TimeUnit.MILLISECONDS);

            } else {
                rate.increment();
                super.onReceiveMessage(msg);
            }
        }
        
        
    }

    public MetricAggregator getTotalConsumerRate() {
        return totalConsumerRate;
    }

    public void setTotalConsumerRate(MetricAggregator totalConsumerRate) {
        this.totalConsumerRate = totalConsumerRate;
    }

    public long getThinkTime() {
        return thinkTime;
    }

    public void setThinkTime(long thinkTime) {
        this.thinkTime = thinkTime;
    }

    public Destination getDestination() {
        return destination;
    }

    public void setDestination(Destination destination) {
        this.destination = destination;
    }

    public String getSelector() {
        return selector;
    }

    public void setSelector(String selector) {
        this.selector = selector;
    }

    public MetricCounter getRate() {
        return rate;
    }
    

}
