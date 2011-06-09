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

package org.apache.activemq.apollo.openwire.support.state;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.apollo.openwire.command.ConsumerId;
import org.apache.activemq.apollo.openwire.command.ConsumerInfo;
import org.apache.activemq.apollo.openwire.command.ProducerId;
import org.apache.activemq.apollo.openwire.command.ProducerInfo;
import org.apache.activemq.apollo.openwire.command.SessionInfo;

public class SessionState {
    final SessionInfo info;

    private final Map<ProducerId, org.apache.activemq.apollo.openwire.support.state.ProducerState> producers = new ConcurrentHashMap<ProducerId, org.apache.activemq.apollo.openwire.support.state.ProducerState>();
    private final Map<ConsumerId, org.apache.activemq.apollo.openwire.support.state.ConsumerState> consumers = new ConcurrentHashMap<ConsumerId, org.apache.activemq.apollo.openwire.support.state.ConsumerState>();
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    public SessionState(SessionInfo info) {
        this.info = info;
    }

    public String toString() {
        return info.toString();
    }

    public void addProducer(ProducerInfo info) {
        checkShutdown();
        producers.put(info.getProducerId(), new org.apache.activemq.apollo.openwire.support.state.ProducerState(info));
    }

    public org.apache.activemq.apollo.openwire.support.state.ProducerState removeProducer(ProducerId id) {
        return producers.remove(id);
    }

    public void addConsumer(ConsumerInfo info) {
        checkShutdown();
        consumers.put(info.getConsumerId(), new org.apache.activemq.apollo.openwire.support.state.ConsumerState(info));
    }

    public org.apache.activemq.apollo.openwire.support.state.ConsumerState removeConsumer(ConsumerId id) {
        return consumers.remove(id);
    }

    public SessionInfo getInfo() {
        return info;
    }

    public Set<ConsumerId> getConsumerIds() {
        return consumers.keySet();
    }

    public Set<ProducerId> getProducerIds() {
        return producers.keySet();
    }

    public Collection<org.apache.activemq.apollo.openwire.support.state.ProducerState> getProducerStates() {
        return producers.values();
    }

    public org.apache.activemq.apollo.openwire.support.state.ProducerState getProducerState(ProducerId producerId) {
        return producers.get(producerId);
    }

    public Collection<org.apache.activemq.apollo.openwire.support.state.ConsumerState> getConsumerStates() {
        return consumers.values();
    }

    public org.apache.activemq.apollo.openwire.support.state.ConsumerState getConsumerState(ConsumerId consumerId) {
        return consumers.get(consumerId);
    }

    private void checkShutdown() {
        if (shutdown.get()) {
            throw new IllegalStateException("Disposed");
        }
    }

    public void shutdown() {
        shutdown.set(false);
    }

}
