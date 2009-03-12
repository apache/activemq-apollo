package org.apache.activemq.broker.openwire;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;

import org.apache.activemq.broker.BrokerConnection;
import org.apache.activemq.broker.DeliveryTarget;
import org.apache.activemq.broker.Destination;
import org.apache.activemq.broker.MessageDelivery;
import org.apache.activemq.broker.Router;
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
import org.apache.activemq.protobuf.AsciiBuffer;
import org.apache.activemq.queue.SingleFlowRelay;
import org.apache.activemq.selector.SelectorParser;
import org.apache.activemq.state.CommandVisitor;

public class OpenwireBrokerConnection extends BrokerConnection {

    protected final HashMap<ProducerId, ProducerContext> producers = new HashMap<ProducerId, ProducerContext>();
    protected final HashMap<ConsumerId, ConsumerContext> consumers = new HashMap<ConsumerId, ConsumerContext>();

    protected final Object inboundMutex = new Object();
    protected IFlowController<MessageDelivery> inboundController;
//    private SingleFlowRelay<MessageDelivery> outboundQueue;

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
                    broker.getRouter().bind(convert(info.getDestination()), ctx);
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
                //    These commands are sent to the broker when it's acting like a 
                //    client to another broker.
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
                write(response);
            } else {
                onException(e);
            }

        }
    }

    // /////////////////////////////////////////////////////////////////
    // Internal Support Methods
    // /////////////////////////////////////////////////////////////////

    private Response ack(Command command) {
        if (command.isResponseRequired()) {
            Response rc = new Response();
            rc.setCorrelationId(command.getCommandId());
            write(rc);
        }
        return null;
    }

    @Override
    public void start() throws Exception {
        super.start();
        BrokerInfo info = new BrokerInfo();
        info.setBrokerId(new BrokerId(broker.getName()));
        info.setBrokerName(broker.getName());
        info.setBrokerURL(broker.getUri());
        write(info);
    }

    static class FlowControllableAdapter implements FlowControllable<MessageDelivery> {
        public void flowElemAccepted(ISourceController<MessageDelivery> controller, MessageDelivery elem) {
        }

        public IFlowSink<MessageDelivery> getFlowSink() {
            return null;
        }

        public IFlowSource<MessageDelivery> getFlowSource() {
            return null;
        }
    }
    
    class ProducerContext {

        private final ProducerInfo info;
        private IFlowController<MessageDelivery> controller;
        private String name;

        public ProducerContext(final ProducerInfo info) {
            this.info = info;
            this.name = info.getProducerId().toString();

            // Openwire only uses credit windows at the producer level for
            // producers that request the feature.
            if (info.getWindowSize() > 0) {
                Flow flow = new Flow(info.getProducerId().toString(), false);
                WindowLimiter<MessageDelivery> limiter = new WindowLimiter<MessageDelivery>(false, flow, info.getWindowSize(), info.getWindowSize() / 2) {
                    @Override
                    protected void sendCredit(int credit) {
                        ProducerAck ack = new ProducerAck(info.getProducerId(), credit);
                        write(ack);
                    }
                };

                controller = new FlowController<MessageDelivery>(new FlowControllableAdapter() {
                    public void flowElemAccepted(ISourceController<MessageDelivery> controller, MessageDelivery elem) {
                        route(controller, elem);
                    }

                    public String toString() {
                        return name;
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

        private SingleFlowRelay<MessageDelivery> queue;
        public ProtocolLimiter<MessageDelivery> limiter;

        public ConsumerContext(final ConsumerInfo info) throws InvalidSelectorException {
            this.info = info;
            this.name = info.getConsumerId().toString();
            selector = parseSelector(info);

            Flow flow = new Flow(name, false);
            limiter = new WindowLimiter<MessageDelivery>(true, flow, info.getPrefetchSize(), info.getPrefetchSize() / 2) {
                public int getElementSize(MessageDelivery m) {
                    return 1;
                }
            };
            queue = new SingleFlowRelay<MessageDelivery>(flow, name + "-outbound", limiter);
            queue.setDrain(new IFlowDrain<MessageDelivery>() {
                public void drain(final MessageDelivery message, ISourceController<MessageDelivery> controller) {
                    Message msg = message.asType(Message.class);
                    MessageDispatch md = new MessageDispatch();
                    md.setConsumerId(info.getConsumerId());
                    md.setMessage(msg);
                    md.setDestination(msg.getDestination());
                    write(md);
                };
            });
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

    }

    protected void route(ISourceController<MessageDelivery> controller, MessageDelivery elem) {
        // TODO:
        // Consider doing some caching of this target list. Most producers
        // always send to
        // the same destination.
        Collection<DeliveryTarget> targets = broker.getRouter().route(elem);

        final Message message = ((OpenWireMessageDelivery) elem).getMessage();
        if (targets != null) {

            if (message.isResponseRequired()) {
                // We need to ack the message once we ensure we won't loose it.
                // We know we won't loose it once it's persisted or delivered to
                // a consumer
                // Setup a callback to get notifed once one of those happens.
                if (message.isPersistent()) {
                    elem.setCompletionCallback(new Runnable() {
                        public void run() {
                            ack(message);
                        }
                    });
                } else {
                    // Let the client know the broker got the message.
                    ack(message);
                }
            }

            // Deliver the message to all the targets..
            for (DeliveryTarget dt : targets) {
                if (dt.match(elem)) {
                    dt.getSink().add(elem, controller);
                }
            }

        } else {
            // Let the client know we got the message even though there
            // were no valid targets to deliver the message to.
            if (message.isResponseRequired()) {
                ack(message);
            }
        }
        controller.elementDispatched(elem);
    }

    protected void initialize() {

        // Setup the inbound processing..
        Flow flow = new Flow(name, false);
        SizeLimiter<MessageDelivery> limiter = new SizeLimiter<MessageDelivery>(inputWindowSize, inputResumeThreshold);
        inboundController = new FlowController<MessageDelivery>(new FlowControllableAdapter() {
            public void flowElemAccepted(ISourceController<MessageDelivery> controller, MessageDelivery elem) {
                route(controller, elem);
            }

            public String toString() {
                return name;
            }
        }, flow, limiter, inboundMutex);

//        Flow ouboundFlow = new Flow(name, false);
//        SizeLimiter<MessageDelivery> outboundLimiter = new SizeLimiter<MessageDelivery>(outputWindowSize, outputResumeThreshold);
//        outboundQueue = new SingleFlowRelay<MessageDelivery>(ouboundFlow, name + "-outbound", outboundLimiter);
//        outboundQueue.setDrain(new IFlowDrain<MessageDelivery>() {
//            public void drain(final MessageDelivery message, ISourceController<MessageDelivery> controller) {
//                write(message);
//            };
//        });
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

}
