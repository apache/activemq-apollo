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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;

import org.apache.activemq.WindowLimiter;
import org.apache.activemq.broker.BrokerConnection;
import org.apache.activemq.broker.BrokerMessageDelivery;
import org.apache.activemq.broker.BrokerSubscription;
import org.apache.activemq.broker.Destination;
import org.apache.activemq.broker.MessageDelivery;
import org.apache.activemq.broker.Router;
import org.apache.activemq.broker.VirtualHost;
import org.apache.activemq.broker.BrokerSubscription.UserAlreadyConnectedException;
import org.apache.activemq.broker.openwire.OpenWireMessageDelivery.PersistListener;
import org.apache.activemq.broker.protocol.ProtocolHandler;
import org.apache.activemq.broker.store.Store.MessageRecord;
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
import org.apache.activemq.command.MessageId;
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
import org.apache.activemq.filter.FilterException;
import org.apache.activemq.filter.LogicExpression;
import org.apache.activemq.filter.NoLocalExpression;
import org.apache.activemq.flow.AbstractLimitedFlowResource;
import org.apache.activemq.flow.Flow;
import org.apache.activemq.flow.FlowController;
import org.apache.activemq.flow.IFlowController;
import org.apache.activemq.flow.IFlowLimiter;
import org.apache.activemq.flow.IFlowResource;
import org.apache.activemq.flow.IFlowSink;
import org.apache.activemq.flow.ISourceController;
import org.apache.activemq.flow.SizeLimiter;
import org.apache.activemq.flow.ISinkController.FlowControllable;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.protobuf.AsciiBuffer;
import org.apache.activemq.protobuf.Buffer;
import org.apache.activemq.selector.SelectorParser;
import org.apache.activemq.state.CommandVisitor;
import org.apache.activemq.transport.WireFormatNegotiator;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.wireformat.WireFormat;

public class OpenwireProtocolHandler implements ProtocolHandler, PersistListener {

    protected final HashMap<ProducerId, ProducerContext> producers = new HashMap<ProducerId, ProducerContext>();
    protected final HashMap<ConsumerId, ConsumerContext> consumers = new HashMap<ConsumerId, ConsumerContext>();

    protected BrokerConnection connection;
    private OpenWireFormat wireFormat;
    private OpenWireFormat storeWireFormat;
    private Router router;
    private VirtualHost host;

    public OpenwireProtocolHandler() {
        setStoreWireFormat(new OpenWireFormat());
    }

    public void start() throws Exception {

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
                    connection.setName(info.getClientId());
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
                    md.setStoreWireFormat(storeWireFormat);

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

    class ProducerContext extends AbstractLimitedFlowResource<OpenWireMessageDelivery> {

        protected final Object inboundMutex = new Object();
        private IFlowController<OpenWireMessageDelivery> controller;
        private String name;

        public ProducerContext(final ProducerInfo info) {
            super(info.getProducerId().toString());
            final Flow flow = new Flow("broker-" + name + "-inbound", false);

            // Openwire only uses credit windows at the producer level for
            // producers that request the feature.
            IFlowLimiter<OpenWireMessageDelivery> limiter;
            if (info.getWindowSize() > 0) {
                limiter = new WindowLimiter<OpenWireMessageDelivery>(false, flow, info.getWindowSize(), info.getWindowSize() / 2) {
                    @Override
                    protected void sendCredit(int credit) {
                        ProducerAck ack = new ProducerAck(info.getProducerId(), credit);
                        connection.write(ack);
                    }
                };
            } else {

                limiter = new SizeLimiter<OpenWireMessageDelivery>(connection.getInputWindowSize(), connection.getInputResumeThreshold());
            }

            controller = new FlowController<OpenWireMessageDelivery>(new FlowControllable<OpenWireMessageDelivery>() {
                public void flowElemAccepted(ISourceController<OpenWireMessageDelivery> controller, OpenWireMessageDelivery msg) {
                    router.route(msg, controller);
                    controller.elementDispatched(msg);
                }

                public IFlowResource getFlowResource() {
                    return ProducerContext.this;
                }
            }, flow, limiter, inboundMutex);

            super.onFlowOpened(controller);
        }
    }

    class ConsumerContext extends AbstractLimitedFlowResource<MessageDelivery> implements ProtocolHandler.ConsumerContext {

        private final ConsumerInfo info;
        private String name;
        private BooleanExpression selector;
        private boolean isDurable;
        private boolean isQueueReceiver;

        private final FlowController<MessageDelivery> controller;
        private final WindowLimiter<MessageDelivery> limiter;

        HashMap<MessageId, SubscriptionDeliveryCallback> pendingMessages = new HashMap<MessageId, SubscriptionDeliveryCallback>();
        LinkedList<MessageId> pendingMessageIds = new LinkedList<MessageId>();

        public ConsumerContext(final ConsumerInfo info) throws FilterException, UserAlreadyConnectedException {
            this.info = info;
            this.name = info.getConsumerId().toString();

            Flow flow = new Flow("broker-" + name + "-outbound", false);
            if (info.isDurable())

                selector = parseSelector(info);
            limiter = new WindowLimiter<MessageDelivery>(true, flow, info.getPrefetchSize(), info.getPrefetchSize() / 2) {
                @Override
                public int getElementSize(MessageDelivery m) {
                    return m.getFlowLimiterSize();
                }
            };

            controller = new FlowController<MessageDelivery>(null, flow, limiter, this);
            controller.useOverFlowQueue(false);
            controller.setExecutor(connection.getDispatcher().createPriorityExecutor(connection.getDispatcher().getDispatchPriorities() - 1));
            super.onFlowOpened(controller);
            
            BrokerSubscription sub = host.createSubscription(this);
            sub.connect(this);
        }

        public boolean offer(final MessageDelivery message, ISourceController<?> source, SubscriptionDeliveryCallback callback) {
            if (!controller.offer(message, source)) {
                return false;
            } else {
                sendInternal(message, controller, callback);
                return true;
            }
        }

        public void add(final MessageDelivery message, ISourceController<?> source, SubscriptionDeliveryCallback callback) {
            controller.add(message, source);
            sendInternal(message, controller, callback);
        }

        private void sendInternal(final MessageDelivery message, ISourceController<?> controller, SubscriptionDeliveryCallback callback) {
            Message msg = message.asType(Message.class);
            MessageDispatch md = new MessageDispatch();
            md.setConsumerId(info.getConsumerId());
            md.setMessage(msg);
            md.setDestination(msg.getDestination());
            // Add to the pending list if persistent and we are durable:
            if (callback != null) {
                synchronized (this) {
                    Object old = pendingMessages.put(msg.getMessageId(), callback);
                    if (old != null) {
                        new Exception("Duplicate message id: " + msg.getMessageId()).printStackTrace();
                    }
                    pendingMessageIds.add(msg.getMessageId());
                    connection.write(md);
                }
            } else {
                connection.write(md);
            }
        }

        public void ack(MessageAck info) {
            // TODO: The pending message queue could probably be optimized to
            // avoid having to create a new list here.
            LinkedList<SubscriptionDeliveryCallback> acked = new LinkedList<SubscriptionDeliveryCallback>();
            synchronized (this) {
                MessageId id = info.getLastMessageId();
                if (isDurable() || isQueueReceiver())
                    while (!pendingMessageIds.isEmpty()) {
                        MessageId pendingId = pendingMessageIds.getFirst();
                        SubscriptionDeliveryCallback callback = pendingMessages.remove(pendingId);
                        acked.add(callback);
                        pendingMessageIds.removeFirst();
                        if (pendingId.equals(id)) {
                            break;
                        }
                    }
                limiter.onProtocolCredit(info.getMessageCount());
            }

            // Delete outside of synchronization on queue to avoid contention
            // with enqueueing threads.
            for (SubscriptionDeliveryCallback callback : acked) {
                callback.acknowledge();
            }
        }

        public boolean hasSelector() {
            return selector != null;
        }

        public boolean matches(MessageDelivery message) {
            Message msg = message.asType(Message.class);
            if (msg == null) {
                return false;
            }

            OpenwireMessageEvaluationContext selectorContext = new OpenwireMessageEvaluationContext(msg);
            selectorContext.setDestination(info.getDestination());
            try {
                return (selector == null || selector.matches(selectorContext));
            } catch (FilterException e) {
                e.printStackTrace();
                return false;
            }
        }

        public boolean isDurable() {
            return info.isDurable();
        }

        public boolean isQueueReceiver() {
            return isQueueReceiver;
        }

        /*
         * (non-Javadoc)
         * 
         * @see org.apache.activemq.queue.Subscription#isBrowser()
         */
        public boolean isBrowser() {
            return info.isBrowser();
        }

        /*
         * (non-Javadoc)
         * 
         * @see
         * org.apache.activemq.queue.Subscription#isRemoveOnDispatch(java.lang
         * .Object)
         */
        public boolean isRemoveOnDispatch(MessageDelivery elem) {
            return !elem.isPersistent() || !(isDurable || isQueueReceiver);
        }

        /*
         * (non-Javadoc)
         * 
         * @see
         * org.apache.activemq.broker.protocol.ProtocolHandler.ConsumerContext
         * #getDestination()
         */
        public Destination getDestination() {
            return convert(info.getDestination());
        }

        /*
         * (non-Javadoc)
         * 
         * @see
         * org.apache.activemq.broker.protocol.ProtocolHandler.ConsumerContext
         * #getJMSSelector()
         */
        public String getSelectorString() {
            return info.getSelector();
        }

        /*
         * (non-Javadoc)
         * 
         * @see
         * org.apache.activemq.broker.protocol.ProtocolHandler.ConsumerContext
         * #getSubscriptionName()
         */
        public String getSubscriptionName() {
            return info.getSubscriptionName();
        }

        /*
         * (non-Javadoc)
         * 
         * @see
         * org.apache.activemq.broker.protocol.ProtocolHandler.ConsumerContext
         * #getFullSelector()
         */
        public BooleanExpression getSelectorExpression() {
            return selector;
        }

        /*
         * (non-Javadoc)
         * 
         * @see
         * org.apache.activemq.broker.protocol.ProtocolHandler.ConsumerContext
         * #getJMSSelector()
         */
        public String getSelector() {
            return info.getSelector();
        }

        /*
         * (non-Javadoc)
         * 
         * @see
         * org.apache.activemq.broker.protocol.ProtocolHandler.ConsumerContext
         * #getConnection()
         */
        public BrokerConnection getConnection() {
            return connection;
        }

        /*
         * (non-Javadoc)
         * 
         * @see
         * org.apache.activemq.broker.protocol.ProtocolHandler.ConsumerContext
         * #getConsumerId()
         */
        public String getConsumerId() {
            return name;
        }

        /*
         * (non-Javadoc)
         * 
         * @see org.apache.activemq.queue.Subscription#getSink()
         */
        public IFlowSink<MessageDelivery> getSink() {
            return this;
        }

        /*
         * (non-Javadoc)
         * 
         * @see org.apache.activemq.flow.IFlowSink#add(java.lang.Object,
         * org.apache.activemq.flow.ISourceController)
         */
        public void add(MessageDelivery message, ISourceController<?> source) {
            add(message, source, null);
        }

        /*
         * (non-Javadoc)
         * 
         * @see org.apache.activemq.flow.IFlowSink#offer(java.lang.Object,
         * org.apache.activemq.flow.ISourceController)
         */
        public boolean offer(MessageDelivery message, ISourceController<?> source) {
            return offer(message, source, null);
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
        } else if (dest.isTopic()) {
            domain = Router.TOPIC_DOMAIN;
        } else {
            throw new IllegalArgumentException("Unsupported domain type: " + dest);
        }
        return new Destination.SingleDestination(domain, new AsciiBuffer(dest.getPhysicalName()));
    }

    private static BooleanExpression parseSelector(ConsumerInfo info) throws FilterException {
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
        this.host = connection.getBroker().getDefaultVirtualHost();
        this.router = host.getRouter();
    }

    public void setWireFormat(WireFormat wireFormat) {
        this.wireFormat = (OpenWireFormat) wireFormat;
        setStoreWireFormat(this.wireFormat.copy());
    }

    private void setStoreWireFormat(OpenWireFormat wireFormat) {
        this.storeWireFormat = wireFormat;
        storeWireFormat.setVersion(OpenWireFormat.DEFAULT_VERSION);
        storeWireFormat.setCacheEnabled(false);
        storeWireFormat.setTightEncodingEnabled(false);
        storeWireFormat.setSizePrefixDisabled(false);
    }

    public BrokerMessageDelivery createMessageDelivery(MessageRecord record) throws IOException {
        Buffer buf = record.getBuffer();
        Message message = (Message) storeWireFormat.unmarshal(new ByteSequence(buf.data, buf.offset, buf.length));
        OpenWireMessageDelivery delivery = new OpenWireMessageDelivery(message);
        delivery.setStoreWireFormat(storeWireFormat);
        return delivery;
    }
}
