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
import java.util.concurrent.ConcurrentHashMap;

import javax.jms.JMSException;
import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;

import org.apache.activemq.apollo.WindowLimiter;
import org.apache.activemq.apollo.broker.Broker;
import org.apache.activemq.apollo.broker.BrokerConnection;
import org.apache.activemq.apollo.broker.BrokerMessageDelivery;
import org.apache.activemq.apollo.broker.BrokerSubscription;
import org.apache.activemq.apollo.broker.Destination;
import org.apache.activemq.apollo.broker.MessageDelivery;
import org.apache.activemq.apollo.broker.ProtocolHandler;
import org.apache.activemq.apollo.broker.Router;
import org.apache.activemq.apollo.broker.Transaction;
import org.apache.activemq.apollo.broker.VirtualHost;
import org.apache.activemq.apollo.broker.XidImpl;
import org.apache.activemq.apollo.broker.Transaction.TransactionListener;
import org.apache.activemq.broker.openwire.OpenWireMessageDelivery.PersistListener;
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
import org.apache.activemq.command.RemoveInfo;
import org.apache.activemq.command.RemoveSubscriptionInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.command.ShutdownInfo;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.command.TransactionInfo;
import org.apache.activemq.command.WireFormatInfo;
import org.apache.activemq.dispatch.DispatchPriority;
import org.apache.activemq.filter.BooleanExpression;
import org.apache.activemq.apollo.filter.FilterException;
import org.apache.activemq.apollo.filter.LogicExpression;
import org.apache.activemq.apollo.filter.NoLocalExpression;
import org.apache.activemq.flow.Flow;
import org.apache.activemq.flow.FlowController;
import org.apache.activemq.flow.IFlowController;
import org.apache.activemq.flow.IFlowLimiter;
import org.apache.activemq.flow.IFlowResource;
import org.apache.activemq.flow.ISourceController;
import org.apache.activemq.flow.SizeLimiter;
import org.apache.activemq.flow.ISinkController.FlowControllable;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.selector.SelectorParser;
import org.apache.activemq.state.CommandVisitor;
import org.apache.activemq.transport.WireFormatNegotiator;
import org.fusesource.hawtbuf.Buffer;
import org.apache.activemq.wireformat.WireFormat;
import org.fusesource.hawtdispatch.Dispatch;

public class OpenwireProtocolHandler implements ProtocolHandler, PersistListener {

    protected final HashMap<ConnectionId, ClientContext> connections = new HashMap<ConnectionId, ClientContext>();
    protected final HashMap<SessionId, ClientContext> sessions = new HashMap<SessionId, ClientContext>();
    protected final HashMap<ProducerId, ProducerContext> producers = new HashMap<ProducerId, ProducerContext>();
    protected final HashMap<ConsumerId, ConsumerContext> consumers = new HashMap<ConsumerId, ConsumerContext>();

    protected final ConcurrentHashMap<TransactionId, Transaction> transactions = new ConcurrentHashMap<TransactionId, Transaction>();

    protected BrokerConnection connection;
    private OpenWireFormat wireFormat;
    private OpenWireFormat storeWireFormat;
    private Router router;
    private VirtualHost host;
    private final CommandVisitor visitor;

    ArrayList<ActiveMQDestination> temporaryDestinations = new ArrayList<ActiveMQDestination>();

    public OpenwireProtocolHandler() {
        setStoreWireFormat(new OpenWireFormat());

        visitor = new CommandVisitor() {

            // /////////////////////////////////////////////////////////////////
            // Methods that keep track of the client state
            // /////////////////////////////////////////////////////////////////
            public Response processAddConnection(final ConnectionInfo info) throws Exception {
                if (!connections.containsKey(info.getConnectionId())) {

                    ClientContext connection = new AbstractClientContext<MessageDelivery>(info.getConnectionId().toString(), null) {
                        ConnectionInfo connectionInfo = info;

                        public void close() {
                            super.close();
                            connections.remove(connectionInfo.getConnectionId());
                        }
                    };
                    connections.put(info.getConnectionId(), connection);

                    // Connections have an implicitly created "default" session identified by session id = -1
                    SessionId sessionId = new SessionId(info.getConnectionId(), -1);
                    addSession(sessionId, connection);
                }
                return ack(info);
            }

            public Response processAddSession(final SessionInfo info) throws Exception {
                final SessionId sessionId = info.getSessionId();
                ClientContext connection = connections.get(sessionId.getParentId());
                if (connection == null) {
                    throw new IllegalStateException(host.getHostName() + " Cannot add a session to a connection that had not been registered: " + sessionId.getParentId());
                }

                if (!sessions.containsKey(sessionId)) {
                    addSession(sessionId, connection);
                }

                return ack(info);
            }

            private void addSession(final SessionId sessionId, ClientContext connection) {
                ClientContext session = new AbstractClientContext<MessageDelivery>(sessionId.toString(), connection) {
                    public void close() {
                        super.close();
                        sessions.remove(sessionId);
                    }
                };
                sessions.put(sessionId, session);
            }

            public Response processAddProducer(ProducerInfo info) throws Exception {
                ClientContext session = sessions.get(info.getProducerId().getParentId());
                if (session == null) {
                    throw new IllegalStateException(host.getHostName() + " Cannot add a producer to a session that had not been registered: " + info.getProducerId().getParentId());
                }
                if (!producers.containsKey(info.getProducerId())) {
                    ProducerContext producer = new ProducerContext(info, session);
                }
                return ack(info);
            }

            public Response processAddConsumer(ConsumerInfo info) throws Exception {
                ClientContext session = sessions.get(info.getConsumerId().getParentId());
                if (session == null) {
                    throw new IllegalStateException(host.getHostName() + " Cannot add a consumer to a session that had not been registered: " + info.getConsumerId().getParentId());
                }

                if (!consumers.containsKey(info.getConsumerId())) {
                    ConsumerContext ctx = new ConsumerContext(info, session);
                    ctx.start();
                }

                return ack(info);
            }

            public Response processRemoveConnection(RemoveInfo remove, ConnectionId info, long arg1) throws Exception {
                ClientContext cc = connections.get(info);
                if (cc != null) {
                    cc.close();
                }
                ack(remove);
                return null;
            }

            public Response processRemoveSession(RemoveInfo remove, SessionId info, long arg1) throws Exception {
                ClientContext cc = sessions.get(info);
                if (cc != null) {
                    cc.close();
                }
                ack(remove);
                return null;
            }

            public Response processRemoveProducer(RemoveInfo remove, ProducerId info) throws Exception {
                ClientContext cc = producers.get(info);
                if (cc != null) {
                    cc.close();
                }
                ack(remove);
                return null;
            }

            public Response processRemoveConsumer(RemoveInfo remove, ConsumerId info, long arg1) throws Exception {
                ClientContext cc = consumers.get(info);
                if (cc != null) {
                    cc.close();
                }
                ack(remove);
                return null;
            }

            // /////////////////////////////////////////////////////////////////
            // Message Processing Methods.
            // /////////////////////////////////////////////////////////////////
            public Response processMessage(Message info) throws Exception {
                if (info.getOriginalDestination() == null) {
                    info.setOriginalDestination(info.getDestination());
                }

                ProducerId producerId = info.getProducerId();
                ProducerContext producerContext = producers.get(producerId);

                OpenWireMessageDelivery md = new OpenWireMessageDelivery(info);
                md.setStoreWireFormat(storeWireFormat);
                TransactionId tid = info.getTransactionId();
                if (tid != null) {
                    Transaction t = locateTransaction(tid, true);
                    md.setTransactionId(t.getTid());
                } else {
                    md.setPersistListener(OpenwireProtocolHandler.this);
                }

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

                if (tid != null) {
                    return ack(info);
                } else {
                    return null;
                }
            }

            public Response processMessageAck(MessageAck info) throws Exception {
                ConsumerContext ctx = consumers.get(info.getConsumerId());
                ctx.ack(info);
                return ack(info);
            }

            // Only used when client prefetch is set to zero.
            public Response processMessagePull(MessagePull info) throws Exception {
                return ack(info);
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
                Broker broker = connection.getBroker();
                brokerInfo.setBrokerId(new BrokerId(broker.getName()));
                brokerInfo.setBrokerName(broker.getName());
                if (!broker.getConnectUris().isEmpty()) {
                    brokerInfo.setBrokerURL(broker.getConnectUris().get(0));
                }
                connection.write(brokerInfo);
                return ack(info);
            }

            public Response processShutdown(ShutdownInfo info) throws Exception {
                connection.setStopping();
                return ack(info);
            }

            public Response processKeepAlive(KeepAliveInfo info) throws Exception {
                if (info.isResponseRequired()) {
                    info.setResponseRequired(false);
                    connection.write(info);
                }
                return null;
            }

            public Response processFlush(FlushCommand info) throws Exception {
                return ack(info);
            }

            public Response processConnectionControl(ConnectionControl info) throws Exception {
                if (info != null) {
                    if (info.isFaultTolerant()) {
                        throw new UnsupportedOperationException("Fault Tolerance");
                    }
                }
                return ack(info);
            }

            public Response processConnectionError(ConnectionError info) throws Exception {
                return ack(info);
            }

            public Response processConsumerControl(ConsumerControl info) throws Exception {
                return ack(info);
            }

            // /////////////////////////////////////////////////////////////////
            // Methods for server management
            // /////////////////////////////////////////////////////////////////
            public Response processAddDestination(DestinationInfo info) throws Exception {
                ActiveMQDestination destination = info.getDestination();
                if (destination.isTemporary()) {
                    // Keep track of it so that we can remove them this connection 
                    // shuts down.
                    temporaryDestinations.add(destination);
                }
                host.createQueue(destination);
                return ack(info);
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
                TransactionId tid = info.getTransactionId();

                Transaction t = locateTransaction(tid, false);
                if (t == null) {

                    Buffer xid = null;
                    if (tid.isXATransaction()) {
                        xid = XidImpl.toBuffer((Xid) tid);
                    }
                    t = host.getTransactionManager().createTransaction(xid);
                    transactions.put(tid, t);
                }

                return ack(info);
            }

            public Response processCommitTransactionOnePhase(final TransactionInfo info) throws Exception {
                final TransactionId tid = info.getTransactionId();
                Transaction t = locateTransaction(tid, true);
                
                TransactionListener listener = null;
                if (info.isResponseRequired()) {
                    listener = new TransactionListener() {

                        @Override
                        public void onCommit(Transaction t) {
                            transactions.remove(tid);
                            ack(info);
                        }

                        @Override
                        public void onRollback(Transaction t) {
                            transactions.remove(tid);
                            ExceptionResponse r = new ExceptionResponse(new XAException("RolledBack"));
                            r.setCorrelationId(info.getCommandId());
                            connection.write(r);
                        }

                    };
                }
                
                t.commit(true, listener);
                transactions.remove(tid);
                return null;
            }

            public Response processCommitTransactionTwoPhase(final TransactionInfo info) throws Exception {
                final TransactionId tid = info.getTransactionId();
                Transaction t = locateTransaction(tid, true);

                TransactionListener listener = null;
                if (info.isResponseRequired()) {
                    listener = new TransactionListener() {

                        @Override
                        public void onCommit(Transaction t) {
                            transactions.remove(tid);
                            ack(info);
                        }

                        @Override
                        public void onRollback(Transaction t) {
                            transactions.remove(tid);
                            ExceptionResponse r = new ExceptionResponse(new XAException("RolledBack"));
                            r.setCorrelationId(info.getCommandId());
                            connection.write(r);
                        }

                    };
                }

                t.commit(false, listener);
                return null;
            }

            public Response processEndTransaction(TransactionInfo info) throws Exception {
                //Shouldn't actually do anything, send by client to ensure that it is
                //in sync with broker transaction state. 
                //TODO need to investigate whether this should wait for prior transaction
                //state to flush out?
                new UnsupportedOperationException().printStackTrace();
                return ack(info);
            }

            public Response processForgetTransaction(TransactionInfo info) throws Exception {
                return processRollbackTransaction(info);
            }

            public Response processPrepareTransaction(final TransactionInfo info) throws Exception {
                final TransactionId tid = info.getTransactionId();
                Transaction t = locateTransaction(tid, true);

                TransactionListener listener = null;
                if (info.isResponseRequired()) {
                    listener = new TransactionListener() {

                        @Override
                        public void onPrepared(Transaction t) {
                            ack(info);
                        }
                    };
                }
                t.prepare(listener);
                return null;
            }

            public Response processRecoverTransactions(TransactionInfo info) throws Exception {
                //TODO
                throw new UnsupportedOperationException();
            }

            public Response processRollbackTransaction(final TransactionInfo info) throws Exception {
                final TransactionId tid = info.getTransactionId();
                Transaction t = locateTransaction(tid, true);

                TransactionListener listener = null;
                if (info.isResponseRequired()) {
                    listener = new TransactionListener() {

                        @Override
                        public void onRollback(Transaction t) {
                            ack(info);
                        }
                    };
                }
                t.rollback(listener);
                transactions.remove(tid);
                return null;
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
                return ack(info);
            }
        };
    }

    private Transaction locateTransaction(TransactionId tid, boolean expected) throws XAException, JMSException {
        Transaction t;

        if (tid.isLocalTransaction()) {
            t = transactions.get(tid);
        } else {
            t = host.getTransactionManager().getXATransaction(XidImpl.toBuffer((Xid) tid));
        }

        if (t == null && expected) {
            if (tid.isXATransaction()) {
                XAException e = new XAException("Transaction '" + tid + "' has not been started.");
                e.errorCode = XAException.XAER_NOTA;
                throw e;
            } else {
                throw new JMSException("Transaction '" + tid + "' has not been started.");
            }
        }
        return t;
    }

    public void start() throws Exception {

    }

    public void stop() throws Exception {
    }

    public void onCommand(Object o) {
        boolean responseRequired = false;
        int commandId = 0;
        try {
            Command command = (Command) o;
            commandId = command.getCommandId();
            responseRequired = command.isResponseRequired();
            //System.out.println(o);
            command.visit(visitor);
        } catch (Exception e) {
            if (responseRequired) {
                ExceptionResponse response = new ExceptionResponse(e);
                response.setCorrelationId(commandId);
                connection.write(response);
            } else {
                connection.onException(e);
            }
        } catch (Throwable t) {
            if (responseRequired) {
                ExceptionResponse response = new ExceptionResponse(t);
                response.setCorrelationId(commandId);
                connection.write(response);
            } else {
                connection.onException(new RuntimeException(t));
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

    class ProducerContext extends AbstractClientContext<OpenWireMessageDelivery> {

        protected final Object inboundMutex = new Object();
        private IFlowController<OpenWireMessageDelivery> controller;
        private final ProducerInfo info;

        public ProducerContext(final ProducerInfo info, ClientContext parent) {
            super(info.getProducerId().toString(), parent);
            this.info = info;
            producers.put(info.getProducerId(), this);
            final Flow flow = new Flow("broker-" + super.getResourceName() + "-inbound", false);

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

                limiter = new SizeLimiter<OpenWireMessageDelivery>(1024*64, 1024*32);
            }

            controller = new FlowController<OpenWireMessageDelivery>(new FlowControllable<OpenWireMessageDelivery>() {
                public void flowElemAccepted(ISourceController<OpenWireMessageDelivery> controller, OpenWireMessageDelivery msg) {
                    router.route(msg, controller, true);
                    controller.elementDispatched(msg);
                }

                public IFlowResource getFlowResource() {
                    return ProducerContext.this;
                }
            }, flow, limiter, inboundMutex);

            super.onFlowOpened(controller);
        }

        public void close() {
            super.close();
            producers.remove(info);
        }
    }

    class ConsumerContext extends AbstractClientContext<MessageDelivery> implements ProtocolHandler.ConsumerContext {

        private final ConsumerInfo info;
        private String name;
        private BooleanExpression selector;
        private boolean isDurable;
        private boolean isQueueReceiver;

        private final FlowController<MessageDelivery> controller;
        private final WindowLimiter<MessageDelivery> limiter;

        private HashMap<MessageId, SubscriptionDelivery<MessageDelivery>> pendingMessages = new HashMap<MessageId, SubscriptionDelivery<MessageDelivery>>();
        private LinkedList<MessageId> pendingMessageIds = new LinkedList<MessageId>();
        private BrokerSubscription brokerSubscription;
        private int borrowedLimterCredits;

        public ConsumerContext(final ConsumerInfo info, ClientContext parent) throws Exception {
            super(info.getConsumerId().toString(), parent);
            this.info = info;
            this.name = info.getConsumerId().toString();
            consumers.put(info.getConsumerId(), this);

            Flow flow = new Flow("broker-" + name + "-outbound", false);
            selector = parseSelector(info);
            limiter = new WindowLimiter<MessageDelivery>(true, flow, info.getPrefetchSize(), info.getPrefetchSize() / 2) {
                @Override
                public int getElementSize(MessageDelivery m) {
                    return 1;
                }
            };

            isQueueReceiver = info.getDestination().isQueue();
            if (info.getSubscriptionName() != null) {
                isDurable = true;
            }
            controller = new FlowController<MessageDelivery>(null, flow, limiter, this);
            controller.useOverFlowQueue(false);
            controller.setExecutor(Dispatch.getGlobalQueue());
            super.onFlowOpened(controller);
        }

        public void start() throws Exception {
            brokerSubscription = host.createSubscription(this);
            brokerSubscription.connect(this);
        }

        public boolean offer(final MessageDelivery message, ISourceController<?> source, SubscriptionDelivery<MessageDelivery> callback) {
            if (!controller.offer(message, source)) {
                return false;
            } else {
                sendInternal(message, controller, callback);
                return true;
            }
        }

        public void add(final MessageDelivery message, ISourceController<?> source, SubscriptionDelivery<MessageDelivery> callback) {
            controller.add(message, source);
            sendInternal(message, controller, callback);
        }

        private void sendInternal(final MessageDelivery message, ISourceController<?> controller, SubscriptionDelivery<MessageDelivery> callback) {
            Message msg = message.asType(Message.class);
            MessageDispatch md = new MessageDispatch();
            md.setConsumerId(info.getConsumerId());
            md.setMessage(msg);
            md.setDestination(msg.getDestination());
            // Add to the pending list if persistent and we are durable:
            if (callback != null) {
                if (callback.isRedelivery()) {
                    md.setRedeliveryCounter(1);
                }
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

        public void ack(MessageAck info) throws XAException, JMSException {
            // TODO: The pending message queue could probably be optimized to
            // avoid having to create a new list here.
            int flowCredit = info.getMessageCount();
            if (info.isDeliveredAck()) {
                // This ack is just trying to expand the flow control window size without actually 
                // acking the message.  Keep track of how many limiter credits we borrow since they need
                // to get paid back with real acks later.
                borrowedLimterCredits += flowCredit;
                limiter.onProtocolCredit(flowCredit);
            } else if (info.isStandardAck()) {
                TransactionId tid = info.getTransactionId();
                Transaction transaction = null;
                if (tid != null) {
                    transaction = locateTransaction(tid, true);
                }

                LinkedList<SubscriptionDelivery<MessageDelivery>> acked = new LinkedList<SubscriptionDelivery<MessageDelivery>>();
                synchronized (this) {
                    MessageId id = info.getLastMessageId();
                    if (isDurable() || isQueueReceiver()) {
                        while (!pendingMessageIds.isEmpty()) {
                            MessageId pendingId = pendingMessageIds.getFirst();
                            SubscriptionDelivery<MessageDelivery> callback = pendingMessages.remove(pendingId);
                            acked.add(callback);
                            pendingMessageIds.removeFirst();
                            if (pendingId.equals(id)) {
                                break;
                            }
                        }
                    }

                    // Did we have DeliveredAcks previously sent?  Then the 
                    // the flow window has already been credited.  We need to 
                    // pay back the borrowed limiter credits before giving 
                    // credits directly to the limiter.
                    if (borrowedLimterCredits > 0) {
                        if (flowCredit > borrowedLimterCredits) {
                            flowCredit -= borrowedLimterCredits;
                            borrowedLimterCredits = 0;
                        } else {
                            borrowedLimterCredits -= flowCredit;
                            flowCredit = 0;
                        }
                    }
                    limiter.onProtocolCredit(flowCredit);
                }

                if (transaction == null) {
                    // Delete outside of synchronization on queue to avoid contention
                    // with enqueueing threads.
                    for (SubscriptionDelivery<MessageDelivery> callback : acked) {
                        callback.acknowledge();
                    }
                } else {
                    // Delete outside of synchronization on queue to avoid contention
                    // with enqueueing threads.
                    for (SubscriptionDelivery<MessageDelivery> callback : acked) {
                        transaction.addAck(callback);
                    }
                }
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

        public boolean isExclusive() {
            return info.isExclusive();
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
            if (isQueueReceiver()) {
                return false;
            }
            return !elem.isPersistent() || !isDurable;
        }

        /*
         * (non-Javadoc)
         * 
         * @see
         * org.apache.activemq.broker.protocol.ProtocolHandler.ConsumerContext
         * #getDestination()
         */
        public Destination getDestination() {
            return info.getDestination();
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

        public boolean autoCreateDestination() {
            return true;
        }

        public String toString() {
            return info.getConsumerId().toString();
        }

        public void close() {
            brokerSubscription.disconnect(this);

            if (isDurable() || isQueueReceiver()) {
                LinkedList<SubscriptionDelivery<MessageDelivery>> unacquired = null;

                synchronized (this) {

                    unacquired = new LinkedList<SubscriptionDelivery<MessageDelivery>>();
                    while (!pendingMessageIds.isEmpty()) {
                        MessageId pendingId = pendingMessageIds.getLast();
                        SubscriptionDelivery<MessageDelivery> callback = pendingMessages.remove(pendingId);
                        unacquired.add(callback);
                        pendingMessageIds.removeLast();
                    }
                    limiter.onProtocolCredit(unacquired.size());
                }

                if (unacquired != null) {
                    // Delete outside of synchronization on queue to avoid contention
                    // with enqueueing threads.
                    for (SubscriptionDelivery<MessageDelivery> callback : unacquired) {
                        callback.unacquire(controller);
                    }
                }
            }

            super.close();
            consumers.remove(info.getConsumerId());
        }

        public boolean isPersistent() {
            return true;
        }
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
        Message message = (Message) storeWireFormat.unmarshal(new Buffer(buf.data, buf.offset, buf.length));
        OpenWireMessageDelivery delivery = new OpenWireMessageDelivery(message);
        delivery.setStoreWireFormat(storeWireFormat);
        return delivery;
    }
}
