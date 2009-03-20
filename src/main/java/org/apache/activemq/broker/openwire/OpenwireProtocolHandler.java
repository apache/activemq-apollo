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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;

import org.apache.activemq.WindowLimiter;
import org.apache.activemq.broker.BrokerConnection;
import org.apache.activemq.broker.DeliveryTarget;
import org.apache.activemq.broker.Destination;
import org.apache.activemq.broker.MessageDelivery;
import org.apache.activemq.broker.Router;
import org.apache.activemq.broker.openwire.OpenWireMessageDelivery.PersistListener;
import org.apache.activemq.broker.protocol.ProtocolHandler;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.BrokerId;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.ConnectionControl;
import org.apache.activemq.command.ConnectionError;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerControl;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.ControlCommand;
import org.apache.activemq.command.DestinationInfo;
import org.apache.activemq.command.ExceptionResponse;
import org.apache.activemq.command.FlushCommand;
import org.apache.activemq.command.KeepAliveInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageDispatchNotification;
import org.apache.activemq.command.MessagePull;
import org.apache.activemq.command.ProducerAck;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.RemoveSubscriptionInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.command.ShutdownInfo;
import org.apache.activemq.command.TransactionInfo;
import org.apache.activemq.command.WireFormatInfo;
import org.apache.activemq.filter.BooleanExpression;
import org.apache.activemq.filter.LogicExpression;
import org.apache.activemq.filter.MessageEvaluationContext;
import org.apache.activemq.filter.NoLocalExpression;
import org.apache.activemq.flow.Flow;
import org.apache.activemq.flow.FlowController;
import org.apache.activemq.flow.IFlowController;
import org.apache.activemq.flow.IFlowDrain;
import org.apache.activemq.flow.IFlowSink;
import org.apache.activemq.flow.IFlowSource;
import org.apache.activemq.flow.ISourceController;
import org.apache.activemq.flow.SizeLimiter;
import org.apache.activemq.flow.ISinkController.FlowControllable;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.protobuf.AsciiBuffer;
import org.apache.activemq.queue.SingleFlowRelay;
import org.apache.activemq.selector.SelectorParser;
import org.apache.activemq.state.CommandVisitor;
import org.apache.activemq.transport.WireFormatNegotiator;
import org.apache.activemq.wireformat.WireFormat;

public class OpenwireProtocolHandler implements ProtocolHandler, PersistListener {

    protected final HashMap<ProducerId, ProducerContext> producers = new HashMap<ProducerId, ProducerContext>();
    protected final HashMap<ConsumerId, ConsumerContext> consumers = new HashMap<ConsumerId, ConsumerContext>();

    protected final Object inboundMutex = new Object();
    protected IFlowController<OpenWireMessageDelivery> inboundController;

    protected BrokerConnection connection;
    private OpenWireFormat wireFormat;
    private Router router;

    public void start() throws Exception {
        // Setup the inbound processing..
        final Flow flow = new Flow("broker-" + connection.getName() + "-inbound", false);
        SizeLimiter<OpenWireMessageDelivery> limiter = new SizeLimiter<OpenWireMessageDelivery>(connection.getInputWindowSize(), connection.getInputResumeThreshold());
        inboundController = new FlowController<OpenWireMessageDelivery>(new FlowControllableAdapter() {
            public void flowElemAccepted(ISourceController<OpenWireMessageDelivery> controller, OpenWireMessageDelivery elem) {
                if (elem.isResponseRequired()) {
                    elem.setPersistListener(OpenwireProtocolHandler.this);
                }
                router.route(elem, controller);
                controller.elementDispatched(elem);
            }

            public String toString() {
                return flow.getFlowName();
            }
        }, flow, limiter, inboundMutex);
    }

    public void stop() throws Exception {
    }

    public void onCommand(Object o) {

        final Command command = (Command) o;
        boolean responseRequired = command.isResponseRequired();
        try {
            command.visit(new CommandVisitor() {

                // /////////////////////////////////////////////////////////////////
                // Methods that keep track of the client state
                // /////////////////////////////////////////////////////////////////
                public Response processAddConnection(ConnectionInfo info) throws Exception {
                    return ack(command);
                }

                public Response processAddSession(SessionInfo info) throws Exception {
                    return ack(command);
                }

                public Response processAddProducer(ProducerInfo info) throws Exception {
                    producers.put(info.getProducerId(), new ProducerContext(info));
                    return ack(command);
                }

                public Response processAddConsumer(ConsumerInfo info) throws Exception {
                    ConsumerContext ctx = new ConsumerContext(info);
                    consumers.put(info.getConsumerId(), ctx);
                    router.bind(convert(info.getDestination()), ctx);
                    return ack(command);
                }

                public Response processRemoveConnection(ConnectionId info, long arg1) throws Exception {
                    return ack(command);
                }

                public Response processRemoveSession(SessionId info, long arg1) throws Exception {
                    return ack(command);
                }

                public Response processRemoveProducer(ProducerId info) throws Exception {
                    producers.remove(info);
                    return ack(command);
                }

                public Response processRemoveConsumer(ConsumerId info, long arg1) throws Exception {
                    return ack(command);
                }

                // /////////////////////////////////////////////////////////////////
                // Message Processing Methods.
                // /////////////////////////////////////////////////////////////////
                public Response processMessage(Message info) throws Exception {
                    ProducerId producerId = info.getProducerId();
                    ProducerContext producerContext = producers.get(producerId);

                    OpenWireMessageDelivery md = new OpenWireMessageDelivery(info);

                    // Only producers that are not using a window will block,
                    // and if it blocks.
                    // yes we block the connection's read thread. yes other
                    // sessions will not get
                    // serviced while we block here. The producer is depending
                    // on TCP flow
                    // control to slow him down so we have to stop ready from
                    // the socket at this
                    // point.
                    while (!producerContext.controller.offer(md, null)) {
                        producerContext.controller.waitForFlowUnblock();
                    }
                    return null;
                }

                public Response processMessageAck(MessageAck info) throws Exception {
                    ConsumerContext ctx = consumers.get(info.getConsumerId());
                    ctx.ack(info);
                    return ack(command);
                }

                // Only used when client prefetch is set to zero.
                public Response processMessagePull(MessagePull info) throws Exception {
                    return ack(command);
                }

                // /////////////////////////////////////////////////////////////////
                // Control Methods
                // /////////////////////////////////////////////////////////////////
                public Response processWireFormat(WireFormatInfo info) throws Exception {

                    // Negotiate the openwire encoding options.
                    WireFormatNegotiator wfn = new WireFormatNegotiator(connection.getTransport(), wireFormat, 1);
                    wfn.sendWireFormat();
                    wfn.negociate(info);

                    // Now that the encoding is negotiated.. let the client know
                    // the details about this
                    // broker.
                    BrokerInfo brokerInfo = new BrokerInfo();
                    brokerInfo.setBrokerId(new BrokerId(connection.getBroker().getName()));
                    brokerInfo.setBrokerName(connection.getBroker().getName());
                    brokerInfo.setBrokerURL(connection.getBroker().getBindUri());
                    connection.write(brokerInfo);
                    return ack(command);
                }

                public Response processShutdown(ShutdownInfo info) throws Exception {
                    return ack(command);
                }

                public Response processKeepAlive(KeepAliveInfo info) throws Exception {
                    return ack(command);
                }

                public Response processFlush(FlushCommand info) throws Exception {
                    return ack(command);
                }

                public Response processConnectionControl(ConnectionControl info) throws Exception {
                    return ack(command);
                }

                public Response processConnectionError(ConnectionError info) throws Exception {
                    return ack(command);
                }

                public Response processConsumerControl(ConsumerControl info) throws Exception {
                    return ack(command);
                }

                // /////////////////////////////////////////////////////////////////
                // Methods for server management
                // /////////////////////////////////////////////////////////////////
                public Response processAddDestination(DestinationInfo info) throws Exception {
                    throw new UnsupportedOperationException();
                }

                public Response processRemoveDestination(DestinationInfo info) throws Exception {
                    throw new UnsupportedOperationException();
                }

                public Response processRemoveSubscription(RemoveSubscriptionInfo info) throws Exception {
                    throw new UnsupportedOperationException();
                }

                public Response processControlCommand(ControlCommand info) throws Exception {
                    throw new UnsupportedOperationException();
                }

                // /////////////////////////////////////////////////////////////////
                // Methods for transaction management
                // /////////////////////////////////////////////////////////////////
                public Response processBeginTransaction(TransactionInfo info) throws Exception {
                    throw new UnsupportedOperationException();
                }

                public Response processCommitTransactionOnePhase(TransactionInfo info) throws Exception {
                    throw new UnsupportedOperationException();
                }

                public Response processCommitTransactionTwoPhase(TransactionInfo info) throws Exception {
                    throw new UnsupportedOperationException();
                }

                public Response processEndTransaction(TransactionInfo info) throws Exception {
                    throw new UnsupportedOperationException();
                }

                public Response processForgetTransaction(TransactionInfo info) throws Exception {
                    throw new UnsupportedOperationException();
                }

                public Response processPrepareTransaction(TransactionInfo info) throws Exception {
                    throw new UnsupportedOperationException();
                }

                public Response processRecoverTransactions(TransactionInfo info) throws Exception {
                    throw new UnsupportedOperationException();
                }

                public Response processRollbackTransaction(TransactionInfo info) throws Exception {
                    throw new UnsupportedOperationException();
                }

                // /////////////////////////////////////////////////////////////////
                // Methods for cluster operations
                // These commands are sent to the broker when it's acting like a
                // client to another broker.
                // /////////////////////////////////////////////////////////////////
                public Response processBrokerInfo(BrokerInfo info) throws Exception {
                    throw new UnsupportedOperationException();
                }

                public Response processMessageDispatch(MessageDispatch info) throws Exception {
                    throw new UnsupportedOperationException();
                }

                public Response processMessageDispatchNotification(MessageDispatchNotification info) throws Exception {
                    throw new UnsupportedOperationException();
                }

                public Response processProducerAck(ProducerAck info) throws Exception {
                    return ack(command);
                }

            });
        } catch (Exception e) {
            if (responseRequired) {
                ExceptionResponse response = new ExceptionResponse(e);
                response.setCorrelationId(command.getCommandId());
                connection.write(response);
            } else {
                connection.onException(e);
            }

        }
    }

    public void onException(Exception error) {
        if (!connection.isStopping()) {
            error.printStackTrace();
            new Thread() {
                @Override
                public void run() {
                    try {
                        connection.stop();
                    } catch (Exception ignore) {
                    }
                }
            }.start();
        }
    }
    

    public void onMessagePersisted(OpenWireMessageDelivery delivery) {
        // TODO This method should not block:
        // Either add to output queue, or spin off in a separate thread. 
        ack(delivery.getMessage());
    }

    Response ack(Command command) {
        if (command.isResponseRequired()) {
            Response rc = new Response();
            rc.setCorrelationId(command.getCommandId());
            connection.write(rc);
        }
        return null;
    }

    // /////////////////////////////////////////////////////////////////
    // Internal Support Methods
    // /////////////////////////////////////////////////////////////////

    static class FlowControllableAdapter implements FlowControllable<OpenWireMessageDelivery> {
        public void flowElemAccepted(ISourceController<OpenWireMessageDelivery> controller, OpenWireMessageDelivery elem) {
        }

        public IFlowSink<OpenWireMessageDelivery> getFlowSink() {
            return null;
        }

        public IFlowSource<OpenWireMessageDelivery> getFlowSource() {
            return null;
        }
    }

    class ProducerContext {

        private IFlowController<OpenWireMessageDelivery> controller;
        private String name;

        public ProducerContext(final ProducerInfo info) {
            this.name = info.getProducerId().toString();

            // Openwire only uses credit windows at the producer level for
            // producers that request the feature.
            if (info.getWindowSize() > 0) {
                final Flow flow = new Flow("broker-" + name + "-inbound", false);
                WindowLimiter<OpenWireMessageDelivery> limiter = new WindowLimiter<OpenWireMessageDelivery>(false, flow, info.getWindowSize(), info.getWindowSize() / 2) {
                    @Override
                    protected void sendCredit(int credit) {
                        ProducerAck ack = new ProducerAck(info.getProducerId(), credit);
                        connection.write(ack);
                    }
                };

                controller = new FlowController<OpenWireMessageDelivery>(new FlowControllableAdapter() {
                    public void flowElemAccepted(ISourceController<OpenWireMessageDelivery> controller, OpenWireMessageDelivery msg) {
                        router.route(msg, controller);
                        controller.elementDispatched(msg);
                    }

                    public String toString() {
                        return flow.getFlowName();
                    }
                }, flow, limiter, inboundMutex);
            } else {
                controller = inboundController;
            }
        }
    }

    class ConsumerContext implements DeliveryTarget {

        private final ConsumerInfo info;
        private String name;
        private BooleanExpression selector;
        private boolean durable;

        private SingleFlowRelay<MessageDelivery> queue;
        public WindowLimiter<MessageDelivery> limiter;

        public ConsumerContext(final ConsumerInfo info) throws InvalidSelectorException {
            this.info = info;
            this.name = info.getConsumerId().toString();
            selector = parseSelector(info);

            Flow flow = new Flow("broker-" + name + "-outbound", false);
            limiter = new WindowLimiter<MessageDelivery>(true, flow, info.getPrefetchSize(), info.getPrefetchSize() / 2) {
                public int getElementSize(MessageDelivery m) {
                    return 1;
                }
            };
            queue = new SingleFlowRelay<MessageDelivery>(flow, flow.getFlowName(), limiter);
            queue.setDrain(new IFlowDrain<MessageDelivery>() {
                public void drain(final MessageDelivery message, ISourceController<MessageDelivery> controller) {
                    Message msg = message.asType(Message.class);
                    MessageDispatch md = new MessageDispatch();
                    md.setConsumerId(info.getConsumerId());
                    md.setMessage(msg);
                    md.setDestination(msg.getDestination());
                    connection.write(md);
                };
            });
        }

        public void ack(MessageAck info) {
            synchronized (queue) {
                limiter.onProtocolCredit(info.getMessageCount());
            }
        }

        public IFlowSink<MessageDelivery> getSink() {
            return queue;
        }

        public boolean match(MessageDelivery message) {
            Message msg = message.asType(Message.class);
            if (msg == null) {
                return false;
            }

            MessageEvaluationContext selectorContext = new MessageEvaluationContext();
            selectorContext.setMessageReference(msg);
            selectorContext.setDestination(info.getDestination());
            try {
                return (selector == null || selector.matches(selectorContext));
            } catch (JMSException e) {
                e.printStackTrace();
                return false;
            }
        }

        public boolean isDurable() {
            return durable;
        }

        public AsciiBuffer getPersistentQueueName() {
            return null;
        }

    }

    static public Destination convert(ActiveMQDestination dest) {
        if (dest.isComposite()) {
            ActiveMQDestination[] compositeDestinations = dest.getCompositeDestinations();
            ArrayList<Destination> d = new ArrayList<Destination>();
            for (int i = 0; i < compositeDestinations.length; i++) {
                d.add(convert(compositeDestinations[i]));
            }
            return new Destination.MultiDestination(d);
        }
        AsciiBuffer domain;
        if (dest.isQueue()) {
            domain = Router.QUEUE_DOMAIN;
        }
        if (dest.isTopic()) {
            domain = Router.TOPIC_DOMAIN;
        } else {
            throw new IllegalArgumentException("Unsupported domain type: " + dest);
        }
        return new Destination.SingleDestination(domain, new AsciiBuffer(dest.getPhysicalName()));
    }

    private static BooleanExpression parseSelector(ConsumerInfo info) throws InvalidSelectorException {
        BooleanExpression rc = null;
        if (info.getSelector() != null) {
            rc = SelectorParser.parse(info.getSelector());
        }
        if (info.isNoLocal()) {
            if (rc == null) {
                rc = new NoLocalExpression(info.getConsumerId().getConnectionId());
            } else {
                rc = LogicExpression.createAND(new NoLocalExpression(info.getConsumerId().getConnectionId()), rc);
            }
        }
        if (info.getAdditionalPredicate() != null) {
            if (rc == null) {
                rc = info.getAdditionalPredicate();
            } else {
                rc = LogicExpression.createAND(info.getAdditionalPredicate(), rc);
            }
        }
        return rc;
    }

    public BrokerConnection getConnection() {
        return connection;
    }

    public void setConnection(BrokerConnection connection) {
        this.connection = connection;
        this.router = connection.getBroker().getDefaultVirtualHost().getRouter();
    }

    public void setWireFormat(WireFormat wireFormat) {
        this.wireFormat = (OpenWireFormat) wireFormat;
    }
}
