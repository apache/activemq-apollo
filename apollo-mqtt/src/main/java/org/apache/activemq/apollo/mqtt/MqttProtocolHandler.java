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
import org.apache.activemq.apollo.broker.protocol.AbstractProtocolHandler;
import org.apache.activemq.apollo.broker.protocol.ProtocolFilter3;
import org.apache.activemq.apollo.broker.protocol.ProtocolFilter3$;
import org.apache.activemq.apollo.broker.security.SecurityContext;
import org.apache.activemq.apollo.dto.AcceptingConnectorDTO;
import org.apache.activemq.apollo.dto.ProtocolDTO;
import org.apache.activemq.apollo.mqtt.dto.MqttConnectionStatusDTO;
import org.apache.activemq.apollo.mqtt.dto.MqttDTO;
import org.apache.activemq.apollo.util.*;
import org.fusesource.hawtbuf.UTF8Buffer;
import org.fusesource.hawtdispatch.DispatchQueue;
import org.fusesource.hawtdispatch.Task;
import org.fusesource.hawtdispatch.transport.HeartBeatMonitor;
import org.fusesource.mqtt.codec.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.apache.activemq.apollo.util.Scala2Java.*;
import static org.fusesource.hawtdispatch.Dispatch.NOOP;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class MqttProtocolHandler extends AbstractProtocolHandler {

    public static final Scala2Java.Logger log = new Scala2Java.Logger(Log$.MODULE$.apply(MqttProtocolHandler.class));

    public static <T> T received(T value) {
        log.trace("received: %s", value);
        return value;
    }

    static Fn0<String> WAITING_ON_CLIENT_REQUEST = new Fn0<String>() {
        @Override
        public String apply() {
            return "client request";
        }
    };

    public String protocol() {
        return "mqtt";
    }

    public Broker broker() {
        return connection().connector().broker();
    }

    public DispatchQueue queue() {
        return connection().dispatch_queue();
    }

    Scala2Java.Logger connection_log = log;
    MqttDTO config = null;

    public DestinationParser destination_parser() {
        DestinationParser destination_parser = MqttProtocol.destination_parser;
        if (config.queue_prefix != null ||
                config.path_separator != null ||
                config.any_child_wildcard != null ||
                config.any_descendant_wildcard != null ||
                config.regex_wildcard_start != null ||
                config.regex_wildcard_end != null ||
                config.part_pattern != null
                ) {
            destination_parser = new DestinationParser().copy(destination_parser);
            if (config.queue_prefix != null) {
                destination_parser.queue_prefix_$eq(config.queue_prefix);
            }
            if (config.path_separator != null) {
                destination_parser.path_separator_$eq(config.path_separator);
            }
            if (config.any_child_wildcard != null) {
                destination_parser.any_child_wildcard_$eq(config.any_child_wildcard);
            }
            if (config.any_descendant_wildcard != null) {
                destination_parser.any_descendant_wildcard_$eq(config.any_descendant_wildcard);
            }
            if (config.regex_wildcard_start != null) {
                destination_parser.regex_wildcard_start_$eq(config.regex_wildcard_start);
            }
            if (config.regex_wildcard_end != null) {
                destination_parser.regex_wildcard_end_$eq(config.regex_wildcard_end);
            }
            if (config.part_pattern != null) {
                destination_parser.part_pattern_$eq(Pattern.compile(config.part_pattern));
            }
        }
        return destination_parser;
    }

    final ArrayList<ProtocolFilter3> protocol_filters = new ArrayList<ProtocolFilter3>();

    /////////////////////////////////////////////////////////////////////
    //
    // Bits related setting up a client connection
    //
    /////////////////////////////////////////////////////////////////////
    public String session_id() {
        return security_context.session_id();
    }

    final SecurityContext security_context = new SecurityContext();
    SinkMux<Request> sink_manager = null;
    Sink<Request> connection_sink = null;
    MQTTProtocolCodec codec = null;


    static private MqttDTO find_config(AcceptingConnectorDTO connector_config) {
        for (ProtocolDTO protocol : connector_config.protocols) {
            if (protocol instanceof MqttDTO) {
                return (MqttDTO) protocol;
            }
        }
        return new MqttDTO();
    }

    public void on_transport_connected() {

        codec = (MQTTProtocolCodec) connection().transport().getProtocolCodec();
        AcceptingConnectorDTO connector_config = (AcceptingConnectorDTO) connection().connector().config();
        config = find_config(connector_config);

        codec.setMaxMessageLength(get(config.max_message_length, codec.getMaxMessageLength()));

        protocol_filters.clear();
        protocol_filters.addAll(ProtocolFilter3$.MODULE$.create_filters(config.protocol_filters, this));

        security_context.local_address_$eq(connection().transport().getLocalAddress());
        security_context.remote_address_$eq(connection().transport().getRemoteAddress());
        security_context.connector_id_$eq(connection().connector().id());
        security_context.certificates_$eq(connection().certificates());

        connection_log = new Scala2Java.Logger(connection().connector().broker().connection_log());

        Sink<Request> filtering_sink = new AbstractSinkMapper<Request, Object>() {
            public Sink<Object> downstream() {
                return (Sink<Object>) connection().transport_sink();
            }

            public MQTTFrame passing(Request request) {
                log.trace("sent: %s", request.message);
                request.delivered = true;
                if (request.id == 0 && request.ack != null) {
                    request.ack.apply(Consumed$.MODULE$);
                }
                return request.frame;

            }
        };

        if (!protocol_filters.isEmpty()) {
            final Sink<Request> downstream  = filtering_sink;
            filtering_sink = new AbstractSinkFilter<Request, Request>() {

                @Override
                public Sink<Request> downstream() {
                    return downstream;
                }

                @Override
                public Request filter(Request value) {
                    Request cur = value;
                    for (ProtocolFilter3 filter : protocol_filters) {
                        cur = filter.filter_outbound(cur);
                        if(cur == null ) {
                            break;
                        }
                    }
                    return cur;
                }
            };
        }
        sink_manager = new SinkMux<Request>(filtering_sink);
        connection_sink = new OverflowSink(sink_manager.open());
        resume_read();
    }


    /////////////////////////////////////////////////////////////////////
    //
    // Bits related tearing down a client connection
    //
    /////////////////////////////////////////////////////////////////////
    boolean closed = false;
    public static UnitFn1<Object> dead_handler = new UnitFn1<Object>() {
        @Override
        public void call(Object v1) {
        }
    };

    public void on_transport_disconnected() {
        if (!closed) {
            closed = true;
            dead = true;
            command_handler = dead_handler;

            security_context.logout(toScala(new UnitFn1<Throwable>() {
                @Override
                public void call(Throwable e) {
                    if (e != null) {
                        connection_log.info(e, "MQTT connection '%s' log out error: %s", security_context.remote_address(), e.toString());
                    }
                }
            }));

            heart_beat_monitor.stop();
            if (!connection().stopped()) {
                connection().stop(NOOP);
            }
            log.trace("mqtt protocol resources released");
        }
    }

    public void on_transport_failure(IOException error) {
        if (!dead) {
            command_handler.apply("failure");
            dead = true;
            command_handler = dead_handler;
            if (!connection().stopped()) {
                connection_log.info(error, "Shutting connection '%s'  down due to: %s", security_context.remote_address(), error);
                super.on_transport_failure(error);
            }
        }
    }

    /////////////////////////////////////////////////////////////////////
    //
    // Bits related managing connection flow control
    //
    /////////////////////////////////////////////////////////////////////

    Fn0<String> status = WAITING_ON_CLIENT_REQUEST;

    public void _suspend_read(final String reason) {
        suspend_read(new Fn0<String>() {
            @Override
            public String apply() {
                return reason;
            }
        });
    }

    public void suspend_read(Fn0<String> reason) {
        status = reason;
        connection().transport().suspendRead();
        heart_beat_monitor.suspendRead();
    }

    public void resume_read() {
        status = WAITING_ON_CLIENT_REQUEST;
        connection().transport().resumeRead();
        heart_beat_monitor.resumeRead();
    }

    /////////////////////////////////////////////////////////////////////
    //
    // Bits related to raising connection failure signals
    //
    /////////////////////////////////////////////////////////////////////

    boolean dead = false;

    public long die_delay() {
        return get(config.die_delay, 1000 * 5L);
    }

    class Break extends RuntimeException {
    }

    public void async_die(String msg) {
        async_die(msg, null);
    }

    public void async_die(String msg, Throwable e) {
        try {
            die(msg, e);
        } catch (Break x) {
        }
    }

    public void async_die(MessageSupport.Message response, String msg) {
        try {
            die(response, msg, null);
        } catch (Break x) {
        }
    }

    public <T> T die(String msg) {
        return die(null, msg, null);
    }

    public <T> T die(String msg, Throwable e) {
        return die(null, msg, e);
    }

    public <T> T die(MessageSupport.Message response, String msg) {
        return die(response, msg, null);
    }

    public <T> T die(MessageSupport.Message response, String msg, Throwable e) {
        if (e != null) {
            connection_log.info(e, "MQTT connection '%s' error: %s", security_context.remote_address(), msg, e);
        } else {
            connection_log.info("MQTT connection '%s' error: %s", security_context.remote_address(), msg);
        }
        return die(response);
    }

    public <T> T die(MessageSupport.Message response) {
        if (!dead) {
            command_handler.apply("failure");
            dead = true;
            command_handler = dead_handler;
            status = new Fn0<String>() {
                @Override
                public String apply() {
                    return "shuting down";
                }
            };
            if (response != null) {
                connection().transport().resumeRead();
                connection_sink.offer(new Request((short) 0, response, null));
                // TODO: if there are too many open connections we should just close the connection
                // without waiting for the error to get sent to the client.
                queue().executeAfter(die_delay(), TimeUnit.MILLISECONDS, new Task() {
                    @Override
                    public void run() {
                        connection().stop(NOOP);
                    }
                });
            } else {
                connection().stop(NOOP);
            }
        }
        throw new Break();
    }

    /////////////////////////////////////////////////////////////////////
    //
    // Bits for dispatching client requests.
    //
    /////////////////////////////////////////////////////////////////////
    UnitFn1<Object> command_handler = connect_handler();

    public void on_transport_command(Object command) {
        try {
            if (!protocol_filters.isEmpty()) {
                for (ProtocolFilter3 filter : protocol_filters) {
                    command = filter.filter_inbound(command);
                    if (command==null) {
                        return; // dropping the frame.
                    }
                }
            }
            command_handler.apply(command);
        } catch (Break e) {
        } catch (Exception e) {
            // To avoid double logging to the same log category..
            String msg = "Internal Server Error: " + e;
            if (connection_log != MqttProtocolHandler.log) {
                // but we also want the error on the apollo.log file.
                MqttProtocolHandler.log.warn(e, msg);
            }
            async_die(msg, e);
        }
    }


    /////////////////////////////////////////////////////////////////////
    //
    // Bits related establishing the client connection
    //
    /////////////////////////////////////////////////////////////////////

    CONNECT connect_message = null;
    HeartBeatMonitor heart_beat_monitor = new HeartBeatMonitor();
    VirtualHost host = null;

    public UnitFn1<Object> connect_handler() {
        return new UnitFn1<Object>() {
            @Override
            public void call(Object o) {
                if (o instanceof MQTTFrame) {
                    MQTTFrame command = (MQTTFrame) o;
                    try {
                        if (command.messageType() == CONNECT.TYPE) {
                            connect_message = received(new CONNECT().decode(command));
                            on_mqtt_connect();
                        } else {
                            die("Expecting an MQTT CONNECT message, but got: " + command.getClass());
                        }
                    } catch (java.net.ProtocolException e) {
                        die("Internal Server Error: bad mqtt command: " + command);
                    }
                } else if ("failure".equals(o)) {
                } else {
                    die("Internal Server Error: unexpected mqtt command: " + o.getClass());
                }
            }
        };
    }

    public void on_mqtt_connect() {

        final CONNACK connack = new CONNACK();

        switch(connect_message.version()) {
            case 3:case 4: break;
            default:
                connack.code(CONNACK.Code.CONNECTION_REFUSED_UNACCEPTED_PROTOCOL_VERSION);
                die(connack, "Unsupported protocol version: " + connect_message.version());
        }

        if( (connect_message.clientId() == null || connect_message.clientId().length==0) && !connect_message.cleanSession() ) {
            die(connack, "A clean session must be requested when no client id is provided.");
        }

        UTF8Buffer client_id = connect_message.clientId();
        security_context.user_$eq(Scala2Java.toString(connect_message.userName()));
        security_context.password_$eq(Scala2Java.toString(connect_message.password()));
        security_context.session_id_$eq(client_id.toString());

        final short keep_alive = connect_message.keepAlive();
        if (keep_alive > 0) {
            heart_beat_monitor.setReadInterval(((long) (keep_alive * 1.5)) * 1000);
            heart_beat_monitor.setOnDead(new Task() {
                @Override
                public void run() {
                    async_die("Missed keep alive set to " + keep_alive + " seconds");
                }
            });
        }
        heart_beat_monitor.suspendRead();
        heart_beat_monitor.setTransport(connection().transport());
        heart_beat_monitor.start();

        _suspend_read("virtual host lookup");
        broker().dispatch_queue().execute(new Task() {
            @Override
            public void run() {
                host = connection().connector().broker().get_default_virtual_host();
                queue().execute(new Task() {
                    @Override
                    public void run() {
                        resume_read();
                        if (host == null) {
                            connack.code(CONNACK.Code.CONNECTION_REFUSED_SERVER_UNAVAILABLE);
                            async_die(connack, "Default virtual host not found.");
                        } else if (!host.service_state().is_started()) {
                            connack.code(CONNACK.Code.CONNECTION_REFUSED_SERVER_UNAVAILABLE);
                            async_die(connack, "Default virtual host stopped.");
                        } else {
                            connection_log = new Scala2Java.Logger(host.connection_log());
                            if (host.authenticator() != null && host.authorizer() != null) {
                                _suspend_read("authenticating and authorizing connect");
                                host.authenticator().authenticate(security_context, toScala(new UnitFn1<String>() {
                                    public void call(final String auth_err) {
                                        queue().execute(new Task() {
                                            @Override
                                            public void run() {
                                                if (auth_err != null) {
                                                    connack.code(CONNACK.Code.CONNECTION_REFUSED_BAD_USERNAME_OR_PASSWORD);
                                                    async_die(connack, auth_err + ". Credentials=" + security_context.credential_dump());
                                                } else if (!host.authorizer().can(security_context, "connect", connection().connector())) {
                                                    connack.code(CONNACK.Code.CONNECTION_REFUSED_NOT_AUTHORIZED);
                                                    async_die(connack, String.format("Not authorized to connect to connector '%s'. Principals=", connection().connector().id(), security_context.principal_dump()));
                                                } else if (!host.authorizer().can(security_context, "connect", host)) {
                                                    connack.code(CONNACK.Code.CONNECTION_REFUSED_NOT_AUTHORIZED);
                                                    async_die(connack, String.format("Not authorized to connect to virtual host '%s'. Principals=", host.id(), security_context.principal_dump()));
                                                } else {
                                                    resume_read();
                                                    on_host_connected(host);
                                                }
                                            }
                                        });
                                    }
                                }));
                            } else {
                                on_host_connected(host);
                            }
                        }
                    }
                });
            }
        });
    }

    public void on_host_connected(VirtualHost host) {
        MqttSessionManager.attach(host, connect_message.clientId(), this);
    }

    /////////////////////////////////////////////////////////////////////
    //
    // Other msic bits.
    //
    /////////////////////////////////////////////////////////////////////
    LongCounter messages_sent = new LongCounter(0);
    LongCounter messages_received = new LongCounter(0);
    int subscription_count = 0;

    public MqttConnectionStatusDTO create_connection_status(boolean debug) {
        MqttConnectionStatusDTO rc = new MqttConnectionStatusDTO();
        rc.protocol_version = "3.1";
        rc.messages_sent = messages_sent.get();
        rc.messages_received = messages_received.get();
        rc.subscription_count = subscription_count;
        rc.waiting_on = status.apply();
        return rc;
    }


}
