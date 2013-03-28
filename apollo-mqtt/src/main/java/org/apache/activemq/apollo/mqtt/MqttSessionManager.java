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

import org.apache.activemq.apollo.broker.BindAddress;
import org.apache.activemq.apollo.broker.SimpleAddress;
import org.apache.activemq.apollo.broker.SubscriptionAddress;
import org.apache.activemq.apollo.broker.VirtualHost;
import org.apache.activemq.apollo.broker.store.Store;
import org.apache.activemq.apollo.broker.store.StoreUOW;
import org.apache.activemq.apollo.util.Fn0;
import org.apache.activemq.apollo.util.Scala2Java;
import org.apache.activemq.apollo.util.UnitFn0;
import org.apache.activemq.apollo.util.UnitFn1;
import org.fusesource.hawtbuf.AsciiBuffer;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.UTF8Buffer;
import org.fusesource.hawtbuf.proto.InvalidProtocolBufferException;
import org.fusesource.hawtdispatch.Dispatch;
import org.fusesource.hawtdispatch.DispatchQueue;
import org.fusesource.hawtdispatch.Task;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import scala.Tuple2;
import scala.collection.Seq;

import java.util.HashMap;
import java.util.HashSet;

import static org.fusesource.hawtdispatch.Dispatch.createQueue;

/**
 * Tracks active sessions so that we can ensure that a given
 * session id is only associated with once connection
 * at a time.  If a client tries to establish a 2nd
 * connection, the first one will be closed before the session
 * is switch to the new connection.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class MqttSessionManager {

    public static final Scala2Java.Logger log = MqttProtocolHandler.log;

    static DispatchQueue queue = createQueue("session manager");

    interface StorageStrategy {
        void update(Task cb);
        void destroy(Task cb);
        void create(Store store, UTF8Buffer client_id);
    }

    static class SessionState {
        SubscriptionAddress durable_sub = null;
        java.util.HashMap<UTF8Buffer, Tuple2<Topic, BindAddress>> subscriptions = new java.util.HashMap<UTF8Buffer, Tuple2<Topic, BindAddress>>();
        HashSet<Short> received_message_ids = new HashSet<Short>();
        StorageStrategy strategy = new NoopStrategy();

        class NoopStrategy implements StorageStrategy {

            @Override
            public void create(Store store, UTF8Buffer client_id) {
                if (store != null) {
                    strategy = new StoreStrategy(store, client_id);
                }
            }

            @Override
            public void update(Task cb) {
                cb.run();
            }

            @Override
            public void destroy(Task cb) {
                cb.run();
            }
        }

        class StoreStrategy implements StorageStrategy {

            public final Store store;
            public final UTF8Buffer client_id;
            public final UTF8Buffer session_key;

            public StoreStrategy(Store store, UTF8Buffer client_id) {
                this.store = store;
                this.client_id = client_id;
                this.session_key = new UTF8Buffer("mqtt:" + client_id);
            }

            @Override
            public void create(Store store, UTF8Buffer client_id) {
            }

            @Override
            public void update(final Task cb) {
                StoreUOW uow = store.create_uow();
                SessionPB.Bean session_pb = new SessionPB.Bean();
                session_pb.setClientId(client_id);
                for (Short id : received_message_ids) {
                    session_pb.addReceivedMessageIds(id.intValue());
                }
                for (Tuple2<Topic, BindAddress> entry : subscriptions.values()) {
                    Topic topic = entry._1();
                    BindAddress address = entry._2();
                    TopicPB.Bean topic_pb = new TopicPB.Bean();
                    topic_pb.setName(topic.name());
                    topic_pb.setQos(topic.qos().ordinal());
                    topic_pb.setAddress(new UTF8Buffer(address.toString()));
                    session_pb.addSubscriptions(topic_pb);
                }
                uow.put(session_key, session_pb.freeze().toUnframedBuffer());

                final DispatchQueue current = Dispatch.getCurrentQueue();
                uow.on_complete(Scala2Java.toScala(new UnitFn0() {
                    @Override
                    public void call() {
                        current.execute(new Task() {
                            @Override
                            public void run() {
                                cb.run();
                            }
                        });
                    }
                }));
                uow.release();
            }

            @Override
            public void destroy(final Task cb) {
                StoreUOW uow = store.create_uow();
                uow.put(session_key, null);
                final DispatchQueue current = Dispatch.getCurrentQueue();
                uow.on_complete(Scala2Java.toScala(new UnitFn0() {
                    @Override
                    public void call() {
                        current.execute(new Task() {
                            @Override
                            public void run() {
                                strategy = new NoopStrategy();
                                cb.run();
                            }
                        });
                    }
                }));
                uow.release();
            }

        }
    }


    static public class HostState {

        public final VirtualHost host;
        public final HashMap<UTF8Buffer, SessionState> session_states = new HashMap<UTF8Buffer, SessionState>();
        public final HashMap<UTF8Buffer, MqttSession> sessions = new HashMap<UTF8Buffer, MqttSession>();
        public boolean loaded = false;

        public HostState(VirtualHost host) {
            this.host = host;
        }

        public void on_load(final Task func) {
            if (loaded) {
                func.run();
            } else {
                if (host.store() != null) {
                    // We load all the persisted session's from the host's store when we are first accessed.
                    queue.suspend();
                    host.store().get_prefixed_map_entries(new AsciiBuffer("mqtt:"), Scala2Java.toScala(new UnitFn1<Seq<Tuple2<Buffer, Buffer>>>() {
                        @Override
                        public void call(final Seq<Tuple2<Buffer, Buffer>> entries) {
                            queue.resume();
                            queue.execute(new Task() {
                                @Override
                                public void run() {
                                    for (Tuple2<Buffer, Buffer> entry : Scala2Java.toIterable(entries)) {
                                        try {
                                            Buffer value = entry._2();
                                            SessionPB.Buffer session_pb = SessionPB.FACTORY.parseUnframed(value);
                                            SessionState session_state = new SessionState();
                                            session_state.strategy.create(host.store(), session_pb.getClientId());
                                            if (session_pb.hasReceivedMessageIds()) {
                                                for (Integer i : session_pb.getReceivedMessageIdsList()) {
                                                    session_state.received_message_ids.add(i.shortValue());
                                                }
                                            }
                                            if (session_pb.hasSubscriptions()) {
                                                for (TopicPB.Getter sub : session_pb.getSubscriptionsList()) {
                                                    SimpleAddress address = SimpleAddress.apply(sub.getAddress().toString());
                                                    Topic topic = new Topic(sub.getName(), QoS.values()[sub.getQos()]);
                                                    session_state.subscriptions.put(sub.getName(), new Tuple2<Topic, BindAddress>(topic, address));

                                                }
                                            }
                                            session_states.put(session_pb.getClientId(), session_state);
                                        } catch (InvalidProtocolBufferException e) {
                                            log.warn(e, "Could not load a stored MQTT session");
                                        }
                                    }
                                    loaded = true;
                                    func.run();
                                }
                            });
                        }
                    }));

                } else {
                    loaded = true;
                    func.run();
                }
            }
        }
    }

    static public void attach(final VirtualHost host, final UTF8Buffer client_id, final MqttProtocolHandler handler) {
        queue.execute(new Task() {
            @Override
            public void run() {
                final HostState host_state = host.plugin_state(
                        Scala2Java.toScala(new Fn0<HostState>(){
                            @Override
                            public HostState apply() {
                                return new HostState(host);
                            }
                        }),
                        HostState.class);
                host_state.on_load(new Task() {
                    @Override
                    public void run() {
                        MqttSession assignment = host_state.sessions.get(client_id);
                        if (assignment != null) {
                            assignment.connect(handler);
                        } else {
                            SessionState state;
                            if (handler.connect_message.cleanSession()) {
                                state = host_state.session_states.remove(client_id);
                                if (state == null) {
                                    state = new SessionState();
                                }
                            } else {
                                state = host_state.session_states.get(client_id);
                                if (state == null) {
                                    state = new SessionState();
                                    host_state.session_states.put(client_id, state);
                                }
                            }
                            assignment = new MqttSession(host_state, client_id, state);
                            assignment.connect(handler);
                            host_state.sessions.put(client_id, assignment);
                        }
                    }
                });
            }
        });
    }

    static public void disconnect(final HostState host_state, final UTF8Buffer client_id, final MqttProtocolHandler handler) {
        queue.execute(new Task() {
            @Override
            public void run() {
                MqttSession assignment = host_state.sessions.get(client_id);
                if (assignment != null) {
                    assignment.disconnect(handler);
                }
            }
        });
    }

    static public void remove(final HostState host_state, final UTF8Buffer client_id) {
        queue.execute(new Task() {
            @Override
            public void run() {
                host_state.sessions.remove(client_id);
            }
        });
    }
}
