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

import org.apache.activemq.apollo.broker.*;
import org.apache.activemq.apollo.broker.protocol.RawMessage;
import org.apache.activemq.apollo.broker.protocol.RawMessageCodec$;
import org.apache.activemq.apollo.broker.security.SecurityContext;
import org.apache.activemq.apollo.broker.store.StoreUOW;
import org.apache.activemq.apollo.filter.FilterException;
import org.apache.activemq.apollo.mqtt.MqttSessionManager.HostState;
import org.apache.activemq.apollo.mqtt.MqttSessionManager.SessionState;
import org.apache.activemq.apollo.util.*;
import org.apache.activemq.apollo.util.path.Path$;
import org.apache.activemq.apollo.util.path.PathMap;
import org.apache.activemq.apollo.util.path.PathParser;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.UTF8Buffer;
import org.fusesource.hawtdispatch.*;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.fusesource.mqtt.codec.*;
import scala.Option;
import scala.Tuple2;

import java.net.ProtocolException;
import java.util.*;

import static org.fusesource.hawtdispatch.Dispatch.NOOP;
import static org.fusesource.hawtdispatch.Dispatch.createQueue;

import static org.apache.activemq.apollo.mqtt.MqttProtocolHandler.*;

/**
 * An MqttSession can be switch from one connection/protocol handler to another,
 * but it will only be associated with one at a time. An MqttSession tracks
 * the state of the communication with a client.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class MqttSession {

    public final HostState host_state;
    public final UTF8Buffer client_id;
    public final SessionState session_state;
    public final DispatchQueue queue;

    public MqttSession(HostState host_state, UTF8Buffer client_id, SessionState session_state) {
        this.host_state = host_state;
        this.client_id = client_id;
        this.queue = createQueue("mqtt: " + client_id);
        this.session_state = session_state;
    }

    public boolean manager_disconnected = false;
    public MqttProtocolHandler handler;
    public SecurityContext security_context;
    public boolean clean_session = false;
    public CONNECT connect_message;
    public DestinationParser destination_parser = MqttProtocol.destination_parser;
    boolean publish_body = false;

    public VirtualHost host() {
        return host_state.host;
    }

    public void connect(final MqttProtocolHandler next) {
        queue.execute(new Task() {
            public void run() {
                if (manager_disconnected) {
                    // we are not the assignment anymore.. go to the session manager
                    // again to setup a new session.
                    MqttSessionManager.attach(host(), client_id, next);
                } else {

                    // so that we don't switch again until this current switch completes
                    queue.suspend();
                    if (handler != null) {
                        detach();
                        handler = null;
                    }
                    queue.execute(new Task() {
                        public void run() {
                            handler = next;
                            attach();
                        }
                    });

                    // switch the connection to the session queue..
                    next.connection()._set_dispatch_queue(queue, new Task() {
                        public void run() {
                            queue.resume();
                        }
                    });
                }

            }
        });
    }

    public void disconnect(final MqttProtocolHandler prev) {
        queue.execute(new Task() {
            @Override
            public void run() {
                if (handler == prev) {
                    MqttSessionManager.remove(host_state, client_id);
                    manager_disconnected = true;
                    detach();
                    handler = null;
                }
            }
        });
    }

    /////////////////////////////////////////////////////////////////////
    //
    // Bits that deal with connections attaching/detaching from the session
    //
    /////////////////////////////////////////////////////////////////////
    public void attach() {
        queue.assertExecuting();
        final MqttProtocolHandler h = handler;
        clean_session = h.connect_message.cleanSession();
        security_context = h.security_context;
        h.command_handler = new UnitFn1<Object>() {
            @Override
            public void call(Object v1) {
                on_transport_command(v1);
            }
        };

        destination_parser = h.destination_parser();
        mqtt_consumer().consumer_sink.downstream_$eq(Scala2Java.some(h.sink_manager.open()));

        final Task ack_connect = new Task() {
            @Override
            public void run() {
                queue.assertExecuting();
                connect_message = h.connect_message;
                CONNACK connack = new CONNACK();
                connack.code(CONNACK.Code.CONNECTION_ACCEPTED);
                send(connack);
            }
        };

        if (!clean_session) {
            // Setup the previous subscriptions..
            session_state.strategy.create(host().store(), client_id);
            if (!session_state.subscriptions.isEmpty()) {
                h._suspend_read("subscribing");
                ArrayList<Topic> topics = Scala2Java.map(session_state.subscriptions.values(), new Fn1<Tuple2<Topic, BindAddress>, Topic>() {
                    @Override
                    public Topic apply(Tuple2<Topic, BindAddress> v1) {
                        return v1._1();
                    }
                });
                subscribe(topics, new Task() {
                    @Override
                    public void run() {
                        h.resume_read();
                        h.queue().execute(ack_connect);
                    }
                });
            } else {
                ack_connect.run();
            }
        } else {
            // do we need to clear the received ids?
            // durable_session_state.received_message_ids.clear()
            session_state.subscriptions.clear();
            if (session_state.durable_sub != null) {
                final DestinationAddress[] addresses = new DestinationAddress[]{session_state.durable_sub};
                session_state.durable_sub = null;
                host().dispatch_queue().execute(new Task() {
                    @Override
                    public void run() {
                        host().router().delete(addresses, security_context);
                    }
                });
            }

            session_state.strategy.destroy(new Task() {
                @Override
                public void run() {
                    ack_connect.run();
                }
            });
        }

    }

    public void detach() {
        queue.assertExecuting();
        if (!producerRoutes.isEmpty()) {
            final ArrayList<MqttProducerRoute> routes = new ArrayList<MqttProducerRoute>(producerRoutes.values());
            host().dispatch_queue().execute(new Task() {
                @Override
                public void run() {
                    for (MqttProducerRoute route : routes) {
                        host().router().disconnect(new ConnectAddress[]{route.address}, route);
                    }
                }
            });
            producerRoutes.clear();
        }

        if (clean_session) {
            if (!mqtt_consumer().addresses.isEmpty()) {
                final BindAddress[] addresses = mqtt_consumer().addresses.keySet().toArray(new BindAddress[mqtt_consumer().addresses.size()]);
                host().dispatch_queue().execute(new Runnable() {
                    @Override
                    public void run() {
                        host().router().unbind(addresses, mqtt_consumer(), false, security_context);
                    }
                });
                mqtt_consumer().addresses.clear();
            }
            session_state.subscriptions.clear();
        } else {
            if (session_state.durable_sub != null) {
                final BindAddress[] addresses = new BindAddress[]{session_state.durable_sub};
                host().dispatch_queue().execute(new Runnable() {
                    @Override
                    public void run() {
                        host().router().unbind(addresses, mqtt_consumer(), false, security_context);
                    }
                });
                mqtt_consumer().addresses.clear();
                session_state.durable_sub = null;
            }
        }

        for (Request request : in_flight_publishes.values()) {
            if (request.ack != null) {
                request.ack.apply(
                        request.delivered ? Delivered$.MODULE$ : Undelivered$.MODULE$
                );
            }
        }
        in_flight_publishes.clear();

        handler.sink_manager.close(mqtt_consumer().consumer_sink.downstream().get(), Scala2Java.<Request>noopFn1());
        mqtt_consumer().consumer_sink.downstream_$eq(Scala2Java.<Sink<Request>>none());
        handler.on_transport_disconnected();
    }

    public SimpleAddress decode_destination(UTF8Buffer value) {

        SimpleAddress rc = destination_parser.decode_single_destination(value.toString(), Scala2Java.toScala(new Fn1<String, SimpleAddress>() {
            public SimpleAddress apply(String name) {
                return new SimpleAddress("topic", destination_parser.decode_path(name));
            }
        }));
        if (rc == null && handler != null) {
            handler.die("Invalid mqtt destination name: " + value);
        }
        return rc;
    }

    /////////////////////////////////////////////////////////////////////
    //
    // Bits that deal with assigning message ids to QoS > 0 requests
    // and tracking those requests so that they can get replayed on a
    // reconnect.
    //
    /////////////////////////////////////////////////////////////////////

    final HashMap<Short, Request> in_flight_publishes = new HashMap<Short, Request>();

    public void send(MessageSupport.Message message) {
        queue.assertExecuting();
        handler.connection_sink.offer(new Request((short) 0, message, null));
    }

    public void publish_completed(short id) {
        queue.assertExecuting();
        Request request = in_flight_publishes.remove(id);
        if (request != null) {
            if (request.ack != null) {
                request.ack.apply(Consumed$.MODULE$);
            }
        } else {
            // It's possible that on a reconnect, we get an ACK
            // in for message that was not dispatched yet. store
            // a place holder so we ack it upon the dispatch
            // attempt.
            in_flight_publishes.put(id, new Request(id, null, null));
        }
    }

    /////////////////////////////////////////////////////////////////////
    //
    // Bits that deal with processing new messages from the client.
    //
    /////////////////////////////////////////////////////////////////////
    public void on_transport_command(Object o) {
        try {
            if (o.getClass() == MQTTFrame.class) {
                MQTTFrame command = (MQTTFrame) o;
                switch (command.messageType()) {
                    case PUBLISH.TYPE: {
                        on_mqtt_publish(received(new PUBLISH().decode(command)));
                        break;
                    }
                    // This follows a Publish with QoS EXACTLY_ONCE
                    case PUBREL.TYPE: {
                        final PUBREL ack = received(new PUBREL().decode(command));
                        // TODO: perhaps persist the processed list.. otherwise
                        // we can't filter out dups after a broker restart.
                        session_state.received_message_ids.remove(ack.messageId());
                        session_state.strategy.update(new Task() {
                            @Override
                            public void run() {
                                send(new PUBCOMP().messageId(ack.messageId()));
                            }
                        });
                        break;
                    }
                    case SUBSCRIBE.TYPE: {
                        on_mqtt_subscribe(received(new SUBSCRIBE().decode(command)));
                        break;
                    }
                    case UNSUBSCRIBE.TYPE: {
                        on_mqtt_unsubscribe(received(new UNSUBSCRIBE().decode(command)));
                        break;
                    }
                    // AT_LEAST_ONCE ack flow for a client subscription
                    case PUBACK.TYPE: {
                        PUBACK ack = received(new PUBACK().decode(command));
                        publish_completed(ack.messageId());
                        break;
                    }
                    // EXACTLY_ONCE ack flow for a client subscription
                    case PUBREC.TYPE: {
                        PUBREC ack = received(new PUBREC().decode(command));
                        send(new PUBREL().messageId(ack.messageId()));
                        break;
                    }
                    case PUBCOMP.TYPE: {
                        PUBCOMP ack = received(new PUBCOMP().decode(command));
                        publish_completed(ack.messageId());
                        break;
                    }
                    case PINGREQ.TYPE: {
                        received(new PINGREQ().decode(command));
                        send(new PINGRESP());
                        break;
                    }
                    case DISCONNECT.TYPE: {
                        received(new DISCONNECT());
                        MqttSessionManager.disconnect(host_state, client_id, handler);
                        break;
                    }
                    default: {
                        handler.die("Invalid MQTT message type: " + command.messageType());
                        break;
                    }
                }
            } else if ("failure".equals(o)) {
                // Publish the client's will
                publish_will(new Task() {
                    @Override
                    public void run() {
                        // then disconnect him.
                        MqttSessionManager.disconnect(host_state, client_id, handler);

                    }
                });
            } else {
                handler.die("Internal Server Error: unexpected mqtt command: " + o.getClass());
            }
        } catch (ProtocolException e) {
            handler.die("Internal Server Error: " + e);
        }
    }

    /////////////////////////////////////////////////////////////////////
    //
    // Bits that deal with processing PUBLISH messages
    //
    /////////////////////////////////////////////////////////////////////
    LRUCache<UTF8Buffer, MqttProducerRoute> producerRoutes = new LRUCache<UTF8Buffer, MqttProducerRoute>(10) {
        @Override
        protected void onCacheEviction(final Map.Entry<UTF8Buffer, MqttProducerRoute> eldest) {
            host().dispatch_queue().execute(new Task() {
                @Override
                public void run() {
                    ConnectAddress[] array = new ConnectAddress[]{eldest.getValue().address};
                    host().router().disconnect(array, eldest.getValue());
                }
            });
        }
    };

    class MqttProducerRoute extends DeliveryProducerRoute {

        public final SimpleAddress address;
        public final MqttProtocolHandler handler;
        boolean suspended = false;

        public MqttProducerRoute(SimpleAddress address, MqttProtocolHandler h) {
            super(host().router());
            this.address = address;
            this.handler = h;
            this.refiller_$eq(new Task() {
                @Override
                public void run() {
                    if (suspended) {
                        suspended = false;
                        handler.resume_read();
                    }
                }
            });
        }

        @Override
        public int send_buffer_size() {
            return handler.codec.getReadBufferSize();
        }

        @Override
        public Option<BrokerConnection> connection() {
            return Scala2Java.some(handler.connection());
        }

        @Override
        public DispatchQueue dispatch_queue() {
            return queue;
        }
    }

    public void on_mqtt_publish(final PUBLISH publish) {

        if ((publish.qos() == QoS.EXACTLY_ONCE) && session_state.received_message_ids.contains(publish.messageId())) {
            PUBREC response = new PUBREC();
            response.messageId(publish.messageId());
            send(response);
            return;
        }

        handler.messages_received.incrementAndGet();

        queue.assertExecuting();
        MqttProducerRoute route = producerRoutes.get(publish.topicName());
        if (route == null) {
            // create the producer route...

            final SimpleAddress destination = decode_destination(publish.topicName());
            final MqttProducerRoute froute = route = new MqttProducerRoute(destination, handler);

            // don't process commands until producer is connected...
            route.handler._suspend_read("route publish lookup");
            host().dispatch_queue().execute(new Task() {

                @Override
                public void run() {
                    host().router().connect(new ConnectAddress[]{destination}, froute, security_context);
                    queue.execute(new Runnable() {
                        @Override
                        public void run() {
                            // We don't care if we are not allowed to send..
                            if (!froute.handler.connection().stopped()) {
                                froute.handler.resume_read();
                                producerRoutes.put(publish.topicName(), froute);
                                send_via_route(froute, publish);
                            }
                        }
                    });
                }
            });
        } else {
            // we can re-use the existing producer route
            send_via_route(route, publish);
        }
    }

    class AtLeastOnceProducerAck extends UnitFn2<DeliveryResult, StoreUOW> {
        public final PUBLISH publish;

        AtLeastOnceProducerAck(PUBLISH publish) {
            this.publish = publish;
        }

        public void call(final DeliveryResult r, final StoreUOW uow) {
            queue.execute(new Task() {
                @Override
                public void run() {
                    PUBACK response = new PUBACK();
                    response.messageId(publish.messageId());
                    send(response);
                }
            });
        }
    }

    class ExactlyOnceProducerAck extends AtLeastOnceProducerAck {

        ExactlyOnceProducerAck(PUBLISH publish) {
            super(publish);
        }

        public void call(final DeliveryResult r, final StoreUOW uow) {
            queue.execute(new Task() {
                @Override
                public void run() {
                    // TODO: perhaps persist the processed list..
                    session_state.received_message_ids.add(publish.messageId());
                    session_state.strategy.update(new Task() {
                        @Override
                        public void run() {
                            PUBREC response = new PUBREC();
                            response.messageId(publish.messageId());
                            send(response);
                        }
                    });
                }
            });
        }
    }

    public void send_via_route(MqttProducerRoute route, PUBLISH publish) {
        queue.assertExecuting();

        AtLeastOnceProducerAck ack = null;
        if (publish.qos() == QoS.AT_LEAST_ONCE) {
            ack = new AtLeastOnceProducerAck(publish);
        } else if (publish.qos() == QoS.EXACTLY_ONCE) {
            ack = new ExactlyOnceProducerAck(publish);
        }

        if (!route.targets().isEmpty()) {
            Delivery delivery = new Delivery();
            delivery.message_$eq(new RawMessage(publish.payload()));
            delivery.persistent_$eq(publish.qos().ordinal() > 0);
            delivery.size_$eq(publish.payload().length);
            delivery.ack_$eq(Scala2Java.toScala(ack));
            if (publish.retain()) {
                if (delivery.size() == 0) {
                    delivery.retain_$eq(RetainRemove$.MODULE$);
                } else {
                    delivery.retain_$eq(RetainSet$.MODULE$);
                }
            }

            // routes can always accept at least 1 delivery...
            assert !route.full();
            route.offer(delivery);
            if (route.full()) {
                // but once it gets full.. suspend to flow control the producer.
                route.suspended = true;
                handler._suspend_read("blocked sending to: " + route.address);
            }

        } else {
            if (ack != null) {
                ack.apply(null, null);
            }
        }
    }

    public void publish_will(final Task complete_close) {
        if (connect_message != null) {
            if (connect_message.willTopic() == null) {
                complete_close.run();
            } else {

                final SimpleAddress destination = decode_destination(connect_message.willTopic());
                final DeliveryProducerRoute prodcuer = new DeliveryProducerRoute(host().router()) {
                    {
                        refiller_$eq(NOOP);
                    }

                    @Override
                    public int send_buffer_size() {
                        return 1024 * 64;
                    }

                    @Override
                    public Option<BrokerConnection> connection() {
                        return handler != null ? Scala2Java.some(handler.connection()) : Scala2Java.<BrokerConnection>none();
                    }

                    @Override
                    public DispatchQueue dispatch_queue() {
                        return queue;
                    }
                };

                host().dispatch_queue().execute(new Task() {
                    @Override
                    public void run() {
                        host().router().connect(new ConnectAddress[]{destination}, prodcuer, security_context);
                        queue.execute(new Task() {
                            @Override
                            public void run() {
                                if (prodcuer.targets().isEmpty()) {
                                    complete_close.run();
                                } else {
                                    Delivery delivery = new Delivery();
                                    delivery.message_$eq(new RawMessage(connect_message.willMessage()));
                                    delivery.size_$eq(connect_message.willMessage().length);
                                    delivery.persistent_$eq(connect_message.willQos().ordinal() > 0);
                                    if (connect_message.willRetain()) {
                                        if (delivery.size() == 0) {
                                            delivery.retain_$eq(RetainRemove$.MODULE$);
                                        } else {
                                            delivery.retain_$eq(RetainSet$.MODULE$);
                                        }
                                    }
                                    delivery.ack_$eq(Scala2Java.toScala(new UnitFn2<DeliveryResult, StoreUOW>() {
                                        @Override
                                        public void call(DeliveryResult x, StoreUOW y) {
                                            host().dispatch_queue().execute(new Task() {
                                                @Override
                                                public void run() {
                                                    host().router().disconnect(new ConnectAddress[]{destination}, prodcuer);
                                                }
                                            });
                                            complete_close.run();
                                        }
                                    }));
                                    handler.messages_received.incrementAndGet();
                                    prodcuer.offer(delivery);
                                }
                            }
                        });
                    }
                });
            }
        }
    }
    /////////////////////////////////////////////////////////////////////
    //
    // Bits that deal with subscriptions
    //
    /////////////////////////////////////////////////////////////////////

    public void on_mqtt_subscribe(final SUBSCRIBE sub) {
        subscribe(Arrays.asList(sub.topics()), new Task() {
            @Override
            public void run() {
                queue.execute(new Task() {
                    @Override
                    public void run() {
                        session_state.strategy.update(new Task() {
                            @Override
                            public void run() {
                                SUBACK suback = new SUBACK();
                                suback.messageId(sub.messageId());

                                byte[] granted = new byte[sub.topics().length];
                                int i = 0;
                                for (Topic topic : sub.topics()) {
                                    granted[i] = (byte) topic.qos().ordinal();
                                    i++;
                                }

                                suback.grantedQos(granted);
                                send(suback);
                            }
                        });
                    }
                });
            }
        });
    }

    public void subscribe(Collection<Topic> topics, final Task on_subscribed) {
        final ArrayList<BindAddress> addresses = Scala2Java.map(topics, new Fn1<Topic, BindAddress>() {
            @Override
            public BindAddress apply(Topic topic) {
                BindAddress address = decode_destination(topic.name());
                session_state.subscriptions.put(topic.name(), new Tuple2<Topic, BindAddress>(topic, address));
                mqtt_consumer().addresses.put(address, topic.qos());
                if (PathParser.containsWildCards(address.path())) {
                    mqtt_consumer().wildcards.put(address.path(), topic.qos());
                }
                return address;
            }
        });


        handler.subscription_count = mqtt_consumer().addresses.size();

        if (!clean_session) {
            Set<BindAddress> bindAddressSet = mqtt_consumer().addresses.keySet();
            SubscriptionAddress durable_sub = new SubscriptionAddress(Path$.MODULE$.create(client_id.toString()), null, bindAddressSet.toArray(new BindAddress[bindAddressSet.size()]));
            session_state.durable_sub = durable_sub;
            addresses.clear();
            addresses.add(durable_sub);
        }

        host().dispatch_queue().execute(new Task() {
            @Override
            public void run() {
                for (BindAddress address : addresses) {
                    host().router().bind(new BindAddress[]{address}, mqtt_consumer(), security_context, Scala2Java.<Option<String>>noopFn1());
                }
                on_subscribed.run();
            }
        });
    }

    public void on_mqtt_unsubscribe(final UNSUBSCRIBE unsubscribe) {

        ArrayList<BindAddress> addressesList = Scala2Java.flatMap(Arrays.asList(unsubscribe.topics()), new Fn1<UTF8Buffer, Option<BindAddress>>() {
            @Override
            public Option<BindAddress> apply(UTF8Buffer topicName) {
                Tuple2<Topic, BindAddress> removed = session_state.subscriptions.remove(topicName);
                if (removed != null) {
                    Topic topic = removed._1();
                    BindAddress address = removed._2();
                    mqtt_consumer().addresses.remove(address);
                    if (PathParser.containsWildCards(address.path())) {
                        mqtt_consumer().wildcards.remove(address.path(), topic.qos());
                    }
                    return Scala2Java.some(address);
                } else {
                    return Scala2Java.none();
                }

            }
        });
        final BindAddress[] addresses = addressesList.toArray(new BindAddress[addressesList.size()]);

        handler.subscription_count = mqtt_consumer().addresses.size();

        if (!clean_session) {
            Set<BindAddress> bindAddressSet = mqtt_consumer().addresses.keySet();
            session_state.durable_sub = new SubscriptionAddress(Path$.MODULE$.create(client_id.toString()), null, bindAddressSet.toArray(new BindAddress[bindAddressSet.size()]));
        }

        host().dispatch_queue().execute(new Task() {
            @Override
            public void run() {
                if (clean_session) {
                    host().router().unbind(addresses, mqtt_consumer(), false, security_context);
                } else {
                    if (mqtt_consumer().addresses.isEmpty()) {
                        host().router().unbind(new BindAddress[]{session_state.durable_sub}, mqtt_consumer(), true, security_context);
                        session_state.durable_sub = null;
                    } else {
                        host().router().bind(new BindAddress[]{session_state.durable_sub}, mqtt_consumer(), security_context, Scala2Java.<Option<String>>noopFn1());
                    }
                }
                queue.execute(new Task() {
                    @Override
                    public void run() {
                        session_state.strategy.update(new Task() {
                            @Override
                            public void run() {
                                UNSUBACK ack = new UNSUBACK();
                                ack.messageId(unsubscribe.messageId());
                                send(ack);
                            }
                        });
                    }
                });
            }
        });

    }


    MqttConsumer _mqtt_consumer;

    MqttConsumer mqtt_consumer() {
        if (_mqtt_consumer == null) {
            _mqtt_consumer = new MqttConsumer();
        }
        return _mqtt_consumer;
    }

    class IntPair {
        int _1;
        int _2;

        IntPair(int int1, int int2) {
            this._1 = int1;
            this._2 = int2;
        }
    }

    class MqttConsumer extends AbstractRetainedDeliveryConsumer {

        @Override
        public String toString() {
            return "mqtt client:" + client_id + " remote address: " + security_context.remote_address();
        }

        public DispatchQueue dispatch_queue() {
            return queue;
        }

        public HashMap<BindAddress, QoS> addresses = new HashMap<BindAddress, QoS>();
        public PathMap wildcards = new PathMap<QoS>();

        CustomDispatchSource<IntPair, IntPair> credit_window_source = Dispatch.createSource(new EventAggregator<IntPair, IntPair>() {
            public IntPair mergeEvent(IntPair previous, IntPair event) {
                if (previous == null) {
                    return event;
                } else {
                    return new IntPair(previous._1 + event._1, previous._2 + event._2);
                }
            }

            public IntPair mergeEvents(IntPair previous, IntPair events) {
                return mergeEvent(previous, events);
            }

        }, queue);

        {
            credit_window_source.setEventHandler(new Task() {
                @Override
                public void run() {
                    IntPair data = credit_window_source.getData();
                    credit_window_filter.credit(data._1, data._2);
                }
            });
            credit_window_source.resume();
        }

        //
        MutableSink<Request> consumer_sink = new MutableSink<Request>();

        {
            consumer_sink.downstream_$eq(Scala2Java.<Sink<Request>>none());
        }

        public LongCounter next_seq_id = new LongCounter(0);

        public long get_next_seq_id() {
            return next_seq_id.getAndIncrement();
        }

        short to_message_id(long value) {
            return (short)
                    (0x8000 | // MQTT message ids cannot be zero, so we always set the highest bit.
                            (value & 0x7FFF)); // the lower 15 bits come for the original seq id.
        }

        CreditWindowFilter<Tuple2<Session<Delivery>, Delivery>> credit_window_filter = new CreditWindowFilter<Tuple2<Session<Delivery>, Delivery>>(consumer_sink.flatMap(Scala2Java.toScala(new Fn1<Tuple2<Session<Delivery>, Delivery>, Option<Request>>() {
            public Option<Request> apply(Tuple2<Session<Delivery>, Delivery> event) {
                queue.assertExecuting();
                Session<Delivery> session = event._1();
                final Delivery delivery = event._2();

                session_manager.delivered(session, delivery.size());

                // Look up which QoS we need to send this message with..
                SimpleAddress topic = delivery.sender().head().simple();

                QoS qos = addresses.get(topic);
                if (qos == null) {
                    qos = Scala2Java.<QoS>head(wildcards.get(topic.path()));
                }

                if (qos == null) {
                    acked(delivery, Consumed$.MODULE$);
                    return Scala2Java.none();
                } else {
                    PUBLISH publish = new PUBLISH();
                    publish.topicName(new UTF8Buffer(destination_parser.encode_destination(delivery.sender().head())));
                    if (delivery.redeliveries() > 0) {
                        publish.dup(true);
                    }

                    if (delivery.message().codec() == RawMessageCodec$.MODULE$) {
                        publish.payload(((RawMessage) delivery.message()).payload());
                    } else {
                        if (publish_body) {
                            try {
                                publish.payload(delivery.message().getBodyAs(Buffer.class));
                            } catch (FilterException e) {
                                log.error(e, "Internal Server Error: Could not covert message body to a Buffer");
                            }
                        } else {
                            publish.payload(delivery.message().encoded());
                        }
                    }

                    handler.messages_sent.incrementAndGet();

                    UnitFn1<DeliveryResult> ack = new UnitFn1<DeliveryResult>() {
                        @Override
                        public void call(DeliveryResult result) {
                            acked(delivery, result);
                        }
                    };

                    if (delivery.ack() != null && (qos != QoS.AT_MOST_ONCE)) {
                        publish.qos(qos);
                        short id = to_message_id(clean_session ?
                                get_next_seq_id() : // generate our own seq id.
                                delivery.seq() // use the durable sub's seq id..
                        );

                        publish.messageId(id);
                        Request request = new Request(id, publish, ack);
                        Request prev = in_flight_publishes.put(id, request);
                        if (prev != null) {

                            // A reconnecting client could have acked before
                            // we get dispatched by the durable sub.
                            if (prev.message == null) {
                                in_flight_publishes.remove(id);
                                acked(delivery, Consumed$.MODULE$);
                            } else {
                                // Looks we sent out a msg with that id.  This could only
                                // happen once we send out 0x7FFF message and the first
                                // one has not been acked.
                                handler.async_die("Client not acking regularly.", null);
                            }
                        }
                        return Scala2Java.some(request);

                    } else {
                        // This callback gets executed once the message
                        // sent to the transport.
                        publish.qos(QoS.AT_MOST_ONCE);
                        return Scala2Java.some(new Request((short) 0, publish, ack));
                    }

                }
            }
        })), SessionDeliverySizer.INSTANCE);


        public void acked(Delivery delivery, DeliveryResult result) {
            queue.assertExecuting();
            credit_window_source.merge(new IntPair(delivery.size(), 1));
            if (delivery.ack() != null) {
                delivery.ack().apply(result, null);
            }
        }

        {
            credit_window_filter.credit(handler.codec.getWriteBufferSize() * 2, 1);
        }

        SessionSinkMux<Delivery> session_manager = new SessionSinkMux<Delivery>(credit_window_filter, queue, Delivery$.MODULE$, Integer.MAX_VALUE / 2, receive_buffer_size()) {
            @Override
            public long time_stamp() {
                return host().broker().now();
            }
        };

        private void super_dispose() {
            super.dispose();
        }

        @Override
        protected void dispose() {
            queue.execute(new Task() {
                @Override
                public void run() {
                    super_dispose();
                }
            });
        }

        @Override
        public Option<BrokerConnection> connection() {
            return handler != null ? Scala2Java.some(handler.connection()) : Scala2Java.<BrokerConnection>none();
        }

        @Override
        public int receive_buffer_size() {
            return 1024 * 64;
        }

        @Override
        public boolean is_persistent() {
            return false;
        }

        @Override
        public boolean matches(Delivery message) {
            return true;
        }

        //
        // Each destination we subscribe to will establish a session with us.
        //
        class MqttConsumerSession extends AbstractSessionSinkFilter<Delivery> implements DeliverySession {

            final DeliveryProducer producer;
            final SessionSink<Delivery> downstream;

            MqttConsumerSession(DeliveryProducer producer) {
                producer.dispatch_queue().assertExecuting();
                this.producer = producer;
                downstream = session_manager.open(producer.dispatch_queue());
                retain();
            }

            @Override
            public SessionSink<Delivery> downstream_session_sink() {
                return downstream;
            }

            @Override
            public DeliveryProducer producer() {
                return producer;
            }

            @Override
            public String toString() {
                if (handler == null) {
                    return "unconnected";
                } else {
                    return "connection to " + handler.connection().transport().getRemoteAddress();
                }
            }

            public MqttConsumer consumer() {
                return mqtt_consumer();
            }

            public boolean closed = false;

            public void close() {
                producer.dispatch_queue().assertExecuting();
                if (!closed) {
                    closed = true;
                    dispose();
                }
            }

            public void dispose() {
                session_manager.close(downstream(), Scala2Java.toScala(new UnitFn1<Delivery>() {
                    @Override
                    public void call(Delivery delivery) {
                        // We have been closed so we have to nak any deliveries.
                        if (delivery.ack() != null) {
                            delivery.ack().apply(Undelivered$.MODULE$, delivery.uow());
                        }
                    }
                }));
                release();
            }

            @Override
            public boolean offer(Delivery delivery) {
                if (full()) {
                    return false;
                } else {
                    delivery.message().retain();
                    boolean rc = downstream().offer(delivery);
                    assert rc : "offer should be accepted since it was not full";
                    return true;
                }
            }
        }

        public MqttConsumerSession connect(DeliveryProducer p) {
            return new MqttConsumerSession(p);
        }
    }

}
