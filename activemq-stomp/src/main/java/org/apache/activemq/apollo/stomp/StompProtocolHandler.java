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
package org.apache.activemq.apollo.stomp;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.activemq.apollo.WindowLimiter;
import org.apache.activemq.apollo.broker.BrokerConnection;
import org.apache.activemq.apollo.broker.BrokerMessageDelivery;
import org.apache.activemq.apollo.broker.BrokerSubscription;
import org.apache.activemq.apollo.broker.Destination;
import org.apache.activemq.apollo.broker.MessageDelivery;
import org.apache.activemq.apollo.broker.ProtocolHandler;
import org.apache.activemq.apollo.broker.Router;
import org.apache.activemq.apollo.stomp.Stomp.Headers;
import org.apache.activemq.apollo.stomp.Stomp.Headers.Ack;
import org.apache.activemq.apollo.stomp.Stomp.Headers.Response;
import org.apache.activemq.apollo.stomp.Stomp.Headers.Send;
import org.apache.activemq.apollo.stomp.Stomp.Headers.Subscribe;
import org.apache.activemq.broker.store.Store.MessageRecord;
import org.apache.activemq.filter.BooleanExpression;
import org.apache.activemq.filter.FilterException;
import org.apache.activemq.flow.AbstractLimitedFlowResource;
import org.apache.activemq.flow.Flow;
import org.apache.activemq.flow.FlowController;
import org.apache.activemq.flow.IFlowController;
import org.apache.activemq.flow.IFlowResource;
import org.apache.activemq.flow.ISourceController;
import org.apache.activemq.flow.SizeLimiter;
import org.apache.activemq.flow.ISinkController.FlowControllable;
import org.apache.activemq.queue.QueueDispatchTarget;
import org.apache.activemq.queue.SingleFlowRelay;
import org.apache.activemq.util.FactoryFinder;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.buffer.AsciiBuffer;
import org.apache.activemq.util.buffer.Buffer;
import org.apache.activemq.util.buffer.ByteArrayOutputStream;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import static org.apache.activemq.apollo.stomp.Stomp.Commands.*;
import static org.apache.activemq.apollo.stomp.Stomp.Headers.*;
import static org.apache.activemq.apollo.stomp.Stomp.Responses.*;
import static org.apache.activemq.selector.SelectorParser.*;
import static org.apache.activemq.util.buffer.AsciiBuffer.*;

public class StompProtocolHandler implements ProtocolHandler, StompMessageDelivery.PersistListener {

    private static final Log LOG = LogFactory.getLog(StompProtocolHandler.class);
    
    interface ActionHander {
        public void onStompFrame(StompFrame frame) throws Exception;
    }

    private InboundContext inboundContext;

    protected final HashMap<AsciiBuffer, ActionHander> actionHandlers = new HashMap<AsciiBuffer, ActionHander>();
    protected final HashMap<AsciiBuffer, ConsumerContext> consumers = new HashMap<AsciiBuffer, ConsumerContext>();

    protected BrokerConnection connection;

    // TODO: need to update the FrameTranslator to normalize to new broker API
    // objects instead of to the openwire command set.
    private final FrameTranslator translator = new DefaultFrameTranslator();
    private final FactoryFinder FRAME_TRANSLATOR_FINDER = new FactoryFinder("META-INF/services/org/apache/activemq/broker/stomp/frametranslator/");
    private SingleFlowRelay<MessageDelivery> outboundQueue;
    
    public static final IdGenerator CLIENT_IDS = new IdGenerator();
    
    private final String clientId = CLIENT_IDS.generateId();
    private long messageId;
    
    private HashMap<AsciiBuffer, ConsumerContext> allSentMessageIds = new HashMap<AsciiBuffer, ConsumerContext>();
    private Router router;

    protected FrameTranslator translator(StompFrame frame) {
        try {
            AsciiBuffer header = frame.get(TRANSFORMATION);
            if (header != null) {
                return (FrameTranslator) FRAME_TRANSLATOR_FINDER.newInstance(header.toString());
            }
        } catch (Exception ignore) {
        }
        return translator;
    }

    public StompProtocolHandler() {
        actionHandlers.put(CONNECT, new ActionHander() {
            public void onStompFrame(StompFrame frame) throws Exception {
                StompFrame response = new StompFrame(CONNECTED);
                connection.write(response);
            }
        });
        actionHandlers.put(SEND, new ActionHander() {
            public void onStompFrame(StompFrame frame) throws Exception {
                AsciiBuffer dest = frame.get(Send.DESTINATION);
                Destination destination = translator(frame).convert(dest);

                frame.setAction(MESSAGE);
                frame.getHeaders().put(Stomp.Headers.Message.MESSAGE_ID, ascii(clientId+":"+(messageId++)));
                StompMessageDelivery md = new StompMessageDelivery(frame, destination);
                
                inboundContext.onReceive(md);
            }
        });
        actionHandlers.put(SUBSCRIBE, new ActionHander() {
            public void onStompFrame(StompFrame frame) throws Exception {
                AsciiBuffer subscriptionId = frame.get(Subscribe.ID);
                ConsumerContext ctx = new ConsumerContext(subscriptionId.toString(), frame);
                consumers.put(ctx.stompDestination, ctx);
                ctx.start();
                ack(frame);
            }
        });
        actionHandlers.put(UNSUBSCRIBE, new ActionHander() {
            public void onStompFrame(StompFrame frame) throws Exception {
            }
        });
        actionHandlers.put(ACK, new ActionHander() {
            public void onStompFrame(StompFrame frame) throws Exception {
                frame.get(Ack.MESSAGE_ID);
            }
        });
        actionHandlers.put(DISCONNECT, new ActionHander() {
            public void onStompFrame(StompFrame frame) throws Exception {
            }
        });

        actionHandlers.put(ABORT_TRANSACTION, new ActionHander() {
            public void onStompFrame(StompFrame frame) throws Exception {
            }
        });
        actionHandlers.put(BEGIN_TRANSACTION, new ActionHander() {
            public void onStompFrame(StompFrame frame) throws Exception {
            }
        });
        actionHandlers.put(COMMIT_TRANSACTION, new ActionHander() {
            public void onStompFrame(StompFrame frame) throws Exception {
            }
        });
    }

    public void start() throws Exception {
        inboundContext = new InboundContext();

        Flow outboundFlow = new Flow("broker-" + connection.getName() + "-outbound", false);
        SizeLimiter<MessageDelivery> outLimiter = new SizeLimiter<MessageDelivery>(connection.getOutputWindowSize(), connection.getOutputWindowSize());
        outboundQueue = new SingleFlowRelay<MessageDelivery>(outboundFlow, outboundFlow.getFlowName(), outLimiter);
        outboundQueue.setDrain(new QueueDispatchTarget<MessageDelivery>() {
            public void drain(final MessageDelivery message, final ISourceController<MessageDelivery> controller) {
                StompFrame msg = message.asType(StompFrame.class);
                connection.write(msg, new Runnable() {
                    public void run() {
                        controller.elementDispatched(message);
                    }
                });
            };
        });

    }

    public void stop() throws Exception {
    }

    public void onCommand(Object o) {
        StompFrame command = (StompFrame) o;
        try {
            AsciiBuffer action = command.getAction();
            ActionHander actionHander = actionHandlers.get(action);
            if (actionHander == null) {
                throw new IOException("Unsupported command: " + action);
            }
            actionHander.onStompFrame(command);
        } catch (Exception error) {
            try {

                error.printStackTrace();

                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                PrintWriter stream = new PrintWriter(new OutputStreamWriter(baos, "UTF-8"));
                error.printStackTrace(stream);
                stream.close();

                HashMap<AsciiBuffer, AsciiBuffer> headers = new HashMap<AsciiBuffer, AsciiBuffer>();
                headers.put(Headers.Error.MESSAGE, new AsciiBuffer(error.getMessage()));

                if (command != null) {
                    final AsciiBuffer receiptId = command.get(RECEIPT_REQUESTED);
                    if (receiptId != null) {
                        headers.put(Response.RECEIPT_ID, receiptId);
                    }
                }

                StompFrame errorMessage = new StompFrame(ERROR, headers, baos.toBuffer());
                connection.write(errorMessage);
                connection.stop();
            } catch (Exception ignore) {
            }
        }
    }

    public void onException(Exception error) {
        if (!connection.isStopping()) {
            LOG.debug("Unexpected exception.. closing..", error);
            try {

                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                PrintWriter stream = new PrintWriter(new OutputStreamWriter(baos, "UTF-8"));
                error.printStackTrace(stream);
                stream.close();

                sendError(new AsciiBuffer(error.getMessage()), baos.toBuffer());
                connection.stop();

            } catch (Exception ignore) {
            }
        }
    }

    // /////////////////////////////////////////////////////////////////
    // Internal Support Methods
    // /////////////////////////////////////////////////////////////////

    class InboundContext extends AbstractLimitedFlowResource<StompMessageDelivery> {
        protected final Object inboundMutex = new Object();
        protected IFlowController<StompMessageDelivery> inboundController;

        InboundContext() {
            super("broker-" + connection.getName() + "-inbound");
            // Setup the inbound processing..
            final Flow inboundFlow = new Flow(getResourceName(), false);
            SizeLimiter<StompMessageDelivery> inLimiter = new SizeLimiter<StompMessageDelivery>(connection.getInputWindowSize(), connection.getInputResumeThreshold());
            inboundController = new FlowController<StompMessageDelivery>(new FlowControllable<StompMessageDelivery>() {
                public void flowElemAccepted(ISourceController<StompMessageDelivery> controller, StompMessageDelivery elem) {
                    if (elem.isResponseRequired()) {
                        elem.setPersistListener(StompProtocolHandler.this);
                    }
                    router.route(elem, controller, true);
                    controller.elementDispatched(elem);
                }

                public String toString() {
                    return inboundFlow.getFlowName();
                }

                public IFlowResource getFlowResource() {
                    return InboundContext.this;
                }
            }, inboundFlow, inLimiter, inboundMutex);
            super.onFlowOpened(inboundController);
        }

        public void onReceive(StompMessageDelivery md) throws InterruptedException {
            while (!inboundController.offer(md, null)) {
                inboundController.waitForFlowUnblock();
            }
        }
    }

    class ConsumerContext extends AbstractClientContext<MessageDelivery> implements ProtocolHandler.ConsumerContext {

        private BooleanExpression selector;
        private String selectorString;
        
        private SingleFlowRelay<MessageDelivery> queue;
        public WindowLimiter<MessageDelivery> limiter;
        private FrameTranslator translator;
        private String subscriptionId;
        private AsciiBuffer stompDestination;
        private Destination destination;
        private AsciiBuffer ackMode;

        private LinkedHashMap<AsciiBuffer, SubscriptionDelivery<MessageDelivery>> sentMessageIds = new LinkedHashMap<AsciiBuffer, SubscriptionDelivery<MessageDelivery>>();

        private boolean durable;

        public ConsumerContext(String id, final StompFrame subscribe) throws Exception {
            super(id, null);
            translator = translator(subscribe);

            Map<AsciiBuffer, AsciiBuffer> headers = subscribe.getHeaders();
            stompDestination = headers.get(Subscribe.DESTINATION);
            destination = translator.convert(stompDestination);
            subscriptionId = string(headers.get(Subscribe.ID));
            
            AsciiBuffer requestedAckMode = headers.get(Subscribe.ACK_MODE);
            if (Subscribe.AckModeValues.CLIENT.equals(requestedAckMode)) {
                ackMode = Subscribe.AckModeValues.CLIENT;
            } else if (Subscribe.AckModeValues.INDIVIDUAL.equals(requestedAckMode)) {
                ackMode = Subscribe.AckModeValues.INDIVIDUAL;
                sendError(Subscribe.AckModeValues.INDIVIDUAL + " not supported.");
                connection.stop();
                return;
            } else {
                ackMode = Subscribe.AckModeValues.AUTO;
            }

            selectorString = string(subscribe.get(Subscribe.SELECTOR));
            selector = parseSelector(selectorString);

            if (ackMode != Subscribe.AckModeValues.AUTO) {
                Flow flow = new Flow("broker-" + subscriptionId + "-outbound", false);
                limiter = new WindowLimiter<MessageDelivery>(true, flow, connection.getOutputWindowSize(), connection.getOutputResumeThreshold()) {
                    @Override
                    public int getElementSize(MessageDelivery m) {
                        return m.getFlowLimiterSize();
                    }
                };
                
              //FIXME need to keep track of actual size:
              //And Create a flow controller:
            } else {
                queue = outboundQueue;
            }
            
        }
        
        public void start() throws Exception {
            BrokerSubscription sub = router.getVirtualHost().createSubscription(this);
            sub.connect(this);
        }

        public void ack(StompFrame info) throws Exception {
            if (ackMode == Subscribe.AckModeValues.CLIENT || ackMode == Subscribe.AckModeValues.INDIVIDUAL) {
                int credits = 0;
                synchronized (allSentMessageIds) {
                    AsciiBuffer mid = new AsciiBuffer(info.get(Ack.MESSAGE_ID));
                    for (Iterator<AsciiBuffer> iterator = sentMessageIds.keySet().iterator(); iterator.hasNext();) {
                        AsciiBuffer next = iterator.next();
                        iterator.remove();
                        allSentMessageIds.remove(next);
                        //FIXME need to keep track of actual size:
                        credits++;
                        if (next.equals(mid)) {
                            break;
                        }
                    }

                }
                synchronized (queue) {
                    limiter.onProtocolCredit(credits);
                }

            } else {
                // We should not be getting an ACK.
                sendError("ACK not expected.");
                connection.stop();
            }

        }

        public boolean hasSelector() {
            return false;
        }

        public boolean matches(MessageDelivery message) {
            StompFrame stompMessage = message.asType(StompFrame.class);
            if (stompMessage == null) {
                return false;
            }

            return true;

            // TODO: implement selector bits.
            // Message msg = message.asType(Message.class);
            // if (msg == null) {
            // return false;
            // }
            //
            // // TODO: abstract the Selector bits so that it is not openwire
            // specific.
            // MessageEvaluationContext selectorContext = new
            // MessageEvaluationContext();
            // selectorContext.setMessageReference(msg);
            // selectorContext.setDestination(msg.getDestination());
            // try {
            // return (selector == null || selector.matches(selectorContext));
            // } catch (JMSException e) {
            // e.printStackTrace();
            // return false;
            // }
        }
        
        /* (non-Javadoc)
         * @see org.apache.activemq.broker.protocol.ProtocolHandler.ConsumerContext#send(org.apache.activemq.broker.MessageDelivery, org.apache.activemq.flow.ISourceController, org.apache.activemq.queue.Subscription.SubscriptionDeliveryCallback)
         */
        public void add(MessageDelivery message, ISourceController<?> controller, SubscriptionDelivery<MessageDelivery> callback) {
            addInternal(message, controller, callback);
        }
        
        /* (non-Javadoc)
         * @see org.apache.activemq.queue.Subscription#offer(java.lang.Object, org.apache.activemq.flow.ISourceController, org.apache.activemq.queue.Subscription.SubscriptionDeliveryCallback)
         */
        public boolean offer(MessageDelivery message, ISourceController<?> controller, SubscriptionDelivery<MessageDelivery> callback) {
            //FIXME need a controller:
            return false;
        }
        
        private void addInternal(MessageDelivery message, ISourceController<?> controller, SubscriptionDelivery<MessageDelivery> callback)
        {
            StompFrame frame = message.asType(StompFrame.class);
            if (ackMode == Subscribe.AckModeValues.CLIENT || ackMode == Subscribe.AckModeValues.INDIVIDUAL) {
                synchronized (allSentMessageIds) {
                    AsciiBuffer msgId = message.getMsgId();
                    sentMessageIds.put(msgId, callback);
                    allSentMessageIds.put(msgId, ConsumerContext.this);
                }
            }
            connection.write(frame);
        }
        
        public boolean isDurable() {
            return durable;
        }

        /* (non-Javadoc)
         * @see org.apache.activemq.broker.protocol.ProtocolHandler.ConsumerContext#getConnection()
         */
        public BrokerConnection getConnection() {
            return connection;
        }

        /* (non-Javadoc)
         * @see org.apache.activemq.broker.protocol.ProtocolHandler.ConsumerContext#getDestination()
         */
        public Destination getDestination() {
            return destination;
        }

        /* (non-Javadoc)
         * @see org.apache.activemq.broker.protocol.ProtocolHandler.ConsumerContext#getFullSelector()
         */
        public BooleanExpression getSelectorExpression() {
            return selector;
        }

        /* (non-Javadoc)
         * @see org.apache.activemq.broker.protocol.ProtocolHandler.ConsumerContext#getSelector()
         */
        public String getSelector() {
            return selectorString;
        }

        /* (non-Javadoc)
         * @see org.apache.activemq.broker.protocol.ProtocolHandler.ConsumerContext#getSubscriptionName()
         */
        public String getSubscriptionName() {
            return subscriptionId;
        }

        /* (non-Javadoc)
         * @see org.apache.activemq.broker.protocol.ProtocolHandler.ConsumerContext#getConsumerId()
         */
        public String getConsumerId() {
            return subscriptionId;
        }

        /* (non-Javadoc)
         * @see org.apache.activemq.queue.Subscription#isBrowser()
         */
        public boolean isBrowser() {
            return false;
        }

        /* (non-Javadoc)
         * @see org.apache.activemq.queue.Subscription#isRemoveOnDispatch(java.lang.Object)
         */
        public boolean isRemoveOnDispatch(MessageDelivery elem) {
            //TODO fix this.
            return true;
        }

        /* (non-Javadoc)
         * @see org.apache.activemq.flow.IFlowSink#add(java.lang.Object, org.apache.activemq.flow.ISourceController)
         */
        public void add(MessageDelivery elem, ISourceController<?> source) {
            add(elem, source, null);
        }

        /* (non-Javadoc)
         * @see org.apache.activemq.flow.IFlowSink#offer(java.lang.Object, org.apache.activemq.flow.ISourceController)
         */
        public boolean offer(MessageDelivery elem, ISourceController<?> source) {
            return offer(elem, source, null);
        }

		public boolean autoCreateDestination() {
			return true;
		}

        /* (non-Javadoc)
         * @see org.apache.activemq.queue.Subscription#isExclusive()
         */
        public boolean isExclusive() {
            return false;
        }

        public boolean isPersistent() {
            return false;
        }
    }

    private void sendError(String message) {
        sendError(ascii(message), StompFrame.NO_DATA);
    }

    private void sendError(AsciiBuffer message, Buffer details) {
        HashMap<AsciiBuffer, AsciiBuffer> headers = new HashMap<AsciiBuffer, AsciiBuffer>();
        headers.put(Headers.Error.MESSAGE, message);
        StompFrame errorMessage = new StompFrame(ERROR, headers, details);
        connection.write(errorMessage);
    }

    // Callback from MessageDelivery when message's persistence guarantees are
    // met.
    public void onMessagePersisted(StompMessageDelivery delivery) {
        // TODO this method must not block:
        ack(delivery.getStompFrame());
    }

    void ack(StompFrame frame) {
        ack(frame.get(RECEIPT_REQUESTED));
    }

    private void ack(AsciiBuffer receiptId) {
        if (receiptId != null) {
            StompFrame receipt = new StompFrame();
            receipt.setAction(RECEIPT);
            receipt.setHeaders(new HashMap<AsciiBuffer, AsciiBuffer>(1));
            receipt.put(Response.RECEIPT_ID, receiptId);
            connection.write(receipt);
        }
    }

    private static BooleanExpression parseSelector(String selectorString) throws FilterException {
        if (selectorString == null) {
            return null;
        }
        return parse(selectorString);
    }

    public BrokerConnection getConnection() {
        return connection;
    }

    public void setConnection(BrokerConnection connection) {
        this.connection = connection;
        this.router = connection.getBroker().getDefaultVirtualHost().getRouter();
    }

    public void setWireFormat(WireFormat wireFormat) {
    }

    public BrokerMessageDelivery createMessageDelivery(MessageRecord record) {
        throw new UnsupportedOperationException();
    }
    

}
