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
package org.apache.activemq.broker.stomp;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.jms.InvalidSelectorException;

import org.apache.activemq.WindowLimiter;
import org.apache.activemq.broker.BrokerConnection;
import org.apache.activemq.broker.DeliveryTarget;
import org.apache.activemq.broker.Destination;
import org.apache.activemq.broker.MessageDelivery;
import org.apache.activemq.broker.Router;
import org.apache.activemq.broker.protocol.ProtocolHandler;
import org.apache.activemq.broker.store.Store.MessageRecord;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.filter.BooleanExpression;
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
import org.apache.activemq.transport.stomp.Stomp;
import org.apache.activemq.transport.stomp.StompFrame;
import org.apache.activemq.transport.stomp.StompSubscription;
import org.apache.activemq.util.ByteArrayOutputStream;
import org.apache.activemq.util.FactoryFinder;
import org.apache.activemq.wireformat.WireFormat;

public class StompProtocolHandler implements ProtocolHandler, StompMessageDelivery.PersistListener {

    interface ActionHander {
        public void onStompFrame(StompFrame frame) throws Exception;
    }

    protected final HashMap<String, ActionHander> actionHandlers = new HashMap<String, ActionHander>();
    protected final HashMap<String, ConsumerContext> consumers = new HashMap<String, ConsumerContext>();

    protected final Object inboundMutex = new Object();
    protected IFlowController<StompMessageDelivery> inboundController;

    protected BrokerConnection connection;

    // TODO: need to update the FrameTranslator to normalize to new broker API
    // objects instead of to the openwire command set.
    private final FrameTranslator translator = new LegacyFrameTranslator();
    private final FactoryFinder FRAME_TRANSLATOR_FINDER = new FactoryFinder("META-INF/services/org/apache/activemq/broker/stomp/frametranslator/");
    private SingleFlowRelay<MessageDelivery> outboundQueue;

    private HashMap<AsciiBuffer, ConsumerContext> allSentMessageIds = new HashMap<AsciiBuffer, ConsumerContext>();
    private Router router;

    protected FrameTranslator translator(StompFrame frame) {
        try {
            String header = frame.getHeaders().get(Stomp.Headers.TRANSFORMATION);
            if (header != null) {
                return (FrameTranslator) FRAME_TRANSLATOR_FINDER.newInstance(header);
            }
        } catch (Exception ignore) {
        }
        return translator;
    }

    public StompProtocolHandler() {
        actionHandlers.put(Stomp.Commands.CONNECT, new ActionHander() {
            public void onStompFrame(StompFrame frame) throws Exception {
                StompFrame response = new StompFrame(Stomp.Responses.CONNECTED);
                connection.write(response);
            }
        });
        actionHandlers.put(Stomp.Commands.SEND, new ActionHander() {
            public void onStompFrame(StompFrame frame) throws Exception {
                String dest = frame.getHeaders().get(Stomp.Headers.Send.DESTINATION);
                Destination destination = translator(frame).convertToDestination(StompProtocolHandler.this, dest);

                frame.setAction(Stomp.Responses.MESSAGE);
                StompMessageDelivery md = new StompMessageDelivery(frame, destination);
                while (!inboundController.offer(md, null)) {
                    inboundController.waitForFlowUnblock();
                }
            }
        });
        actionHandlers.put(Stomp.Commands.SUBSCRIBE, new ActionHander() {
            public void onStompFrame(StompFrame frame) throws Exception {
                ConsumerContext ctx = new ConsumerContext(frame);
                consumers.put(ctx.stompDestination, ctx);
                router.bind(ctx.destination, ctx);
                ack(frame);
            }
        });
        actionHandlers.put(Stomp.Commands.UNSUBSCRIBE, new ActionHander() {
            public void onStompFrame(StompFrame frame) throws Exception {
            }
        });
        actionHandlers.put(Stomp.Commands.ACK, new ActionHander() {
            public void onStompFrame(StompFrame frame) throws Exception {
                frame.getHeaders().get(Stomp.Headers.Ack.MESSAGE_ID);
            }
        });
        actionHandlers.put(Stomp.Commands.DISCONNECT, new ActionHander() {
            public void onStompFrame(StompFrame frame) throws Exception {
            }
        });

        actionHandlers.put(Stomp.Commands.ABORT_TRANSACTION, new ActionHander() {
            public void onStompFrame(StompFrame frame) throws Exception {
            }
        });
        actionHandlers.put(Stomp.Commands.BEGIN_TRANSACTION, new ActionHander() {
            public void onStompFrame(StompFrame frame) throws Exception {
            }
        });
        actionHandlers.put(Stomp.Commands.COMMIT_TRANSACTION, new ActionHander() {
            public void onStompFrame(StompFrame frame) throws Exception {
            }
        });
    }

    public void start() throws Exception {
        // Setup the inbound processing..
        final Flow inboundFlow = new Flow("broker-" + connection.getName() + "-inbound", false);
        SizeLimiter<StompMessageDelivery> inLimiter = new SizeLimiter<StompMessageDelivery>(connection.getInputWindowSize(), connection.getInputResumeThreshold());
        inboundController = new FlowController<StompMessageDelivery>(new FlowControllableAdapter() {
            public void flowElemAccepted(ISourceController<StompMessageDelivery> controller, StompMessageDelivery elem) {
                if (elem.isResponseRequired()) {
                    elem.setPersistListener(StompProtocolHandler.this);
                }
                router.route(elem, controller);
                controller.elementDispatched(elem);
            }

            public String toString() {
                return inboundFlow.getFlowName();
            }
        }, inboundFlow, inLimiter, inboundMutex);

        Flow outboundFlow = new Flow("broker-" + connection.getName() + "-outbound", false);
        SizeLimiter<MessageDelivery> outLimiter = new SizeLimiter<MessageDelivery>(connection.getOutputWindowSize(), connection.getOutputWindowSize());
        outboundQueue = new SingleFlowRelay<MessageDelivery>(outboundFlow, outboundFlow.getFlowName(), outLimiter);
        outboundQueue.setDrain(new IFlowDrain<MessageDelivery>() {
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
            String action = command.getAction();
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

                HashMap<String, String> headers = new HashMap<String, String>();
                headers.put(Stomp.Headers.Error.MESSAGE, error.getMessage());

                if (command != null) {
                    final String receiptId = command.getHeaders().get(Stomp.Headers.RECEIPT_REQUESTED);
                    if (receiptId != null) {
                        headers.put(Stomp.Headers.Response.RECEIPT_ID, receiptId);
                    }
                }

                StompFrame errorMessage = new StompFrame(Stomp.Responses.ERROR, headers, baos.toByteArray());
                connection.write(errorMessage);
                connection.stop();
            } catch (Exception ignore) {
            }
        }
    }

    public void onException(Exception error) {
        if (!connection.isStopping()) {
            try {

                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                PrintWriter stream = new PrintWriter(new OutputStreamWriter(baos, "UTF-8"));
                error.printStackTrace(stream);
                stream.close();

                sendError(error.getMessage(), baos.toByteArray());
                connection.stop();

            } catch (Exception ignore) {
            }
        }
    }

    // /////////////////////////////////////////////////////////////////
    // Internal Support Methods
    // /////////////////////////////////////////////////////////////////
    static class FlowControllableAdapter implements FlowControllable<StompMessageDelivery> {
        public void flowElemAccepted(ISourceController<StompMessageDelivery> controller, StompMessageDelivery elem) {
        }

        public IFlowSink<StompMessageDelivery> getFlowSink() {
            return null;
        }

        public IFlowSource<StompMessageDelivery> getFlowSource() {
            return null;
        }
    }

    class ConsumerContext implements DeliveryTarget {

        private BooleanExpression selector;

        private SingleFlowRelay<MessageDelivery> queue;
        public WindowLimiter<MessageDelivery> limiter;
        private FrameTranslator translator;
        private String subscriptionId;
        private String stompDestination;
        private Destination destination;
        private String ackMode;

        private LinkedHashMap<AsciiBuffer, AsciiBuffer> sentMessageIds = new LinkedHashMap<AsciiBuffer, AsciiBuffer>();

        private boolean durable;

        public ConsumerContext(final StompFrame subscribe) throws Exception {
            translator = translator(subscribe);

            Map<String, String> headers = subscribe.getHeaders();
            stompDestination = headers.get(Stomp.Headers.Subscribe.DESTINATION);
            destination = translator.convertToDestination(StompProtocolHandler.this, stompDestination);
            subscriptionId = headers.get(Stomp.Headers.Subscribe.ID);

            ackMode = headers.get(Stomp.Headers.Subscribe.ACK_MODE);
            if (Stomp.Headers.Subscribe.AckModeValues.CLIENT.equals(ackMode)) {
                ackMode = StompSubscription.CLIENT_ACK;
            } else if (Stomp.Headers.Subscribe.AckModeValues.INDIVIDUAL.equals(ackMode)) {
                ackMode = StompSubscription.INDIVIDUAL_ACK;
                sendError(StompSubscription.INDIVIDUAL_ACK + " not supported.");
                connection.stop();
                return;
            } else {
                ackMode = StompSubscription.AUTO_ACK;
            }

            selector = parseSelector(subscribe);

            if (ackMode != StompSubscription.AUTO_ACK) {
                Flow flow = new Flow("broker-" + subscriptionId + "-outbound", false);
                limiter = new WindowLimiter<MessageDelivery>(true, flow, 1000, 500) {
                    public int getElementSize(MessageDelivery m) {
                        return 1;
                    }
                };
                queue = new SingleFlowRelay<MessageDelivery>(flow, flow.getFlowName(), limiter);
                queue.setDrain(new IFlowDrain<MessageDelivery>() {
                    public void drain(final MessageDelivery message, ISourceController<MessageDelivery> controller) {
                        StompFrame frame = message.asType(StompFrame.class);
                        if (ackMode == StompSubscription.CLIENT_ACK || ackMode == StompSubscription.INDIVIDUAL_ACK) {
                            synchronized (allSentMessageIds) {
                                AsciiBuffer msgId = message.getMsgId();
                                sentMessageIds.put(msgId, msgId);
                                allSentMessageIds.put(msgId, ConsumerContext.this);
                            }
                        }
                        connection.write(frame);
                    };
                });
            } else {
                queue = outboundQueue;
            }

        }

        public void ack(StompFrame info) throws Exception {
            if (ackMode == StompSubscription.CLIENT_ACK || ackMode == StompSubscription.INDIVIDUAL_ACK) {
                int credits = 0;
                synchronized (allSentMessageIds) {
                    AsciiBuffer mid = new AsciiBuffer(info.getHeaders().get(Stomp.Headers.Ack.MESSAGE_ID));
                    for (Iterator<AsciiBuffer> iterator = sentMessageIds.keySet().iterator(); iterator.hasNext();) {
                        AsciiBuffer next = iterator.next();
                        iterator.remove();
                        allSentMessageIds.remove(next);
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

        public IFlowSink<MessageDelivery> getSink() {
            return queue;
        }

        public boolean match(MessageDelivery message) {
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

        public boolean isDurable() {
            return durable;
        }

        public AsciiBuffer getPersistentQueueName() {
            return null;
        }

    }

    private void sendError(String message) {
        sendError(message, StompFrame.NO_DATA);
    }

    private void sendError(String message, String details) {
        try {
            sendError(message, details.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    private void sendError(String message, byte[] details) {
        HashMap<String, String> headers = new HashMap<String, String>();
        headers.put(Stomp.Headers.Error.MESSAGE, message);
        StompFrame errorMessage = new StompFrame(Stomp.Responses.ERROR, headers, details);
        connection.write(errorMessage);
    }

    //Callback from MessageDelivery when message's persistence guarantees are met. 
    public void onMessagePersisted(StompMessageDelivery delivery) {
        //TODO this method must not block:
        ack(delivery.getStomeFame());
    }

    void ack(StompFrame frame) {
        ack(frame.getHeaders().get(Stomp.Headers.RECEIPT_REQUESTED));
    }

    private void ack(String receiptId) {
        if (receiptId != null) {
            StompFrame receipt = new StompFrame();
            receipt.setAction(Stomp.Responses.RECEIPT);
            receipt.setHeaders(new HashMap<String, String>(1));
            receipt.getHeaders().put(Stomp.Headers.Response.RECEIPT_ID, receiptId);
            connection.write(receipt);
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

    private static BooleanExpression parseSelector(StompFrame frame) throws InvalidSelectorException {
        BooleanExpression rc = null;
        String selector = frame.getHeaders().get(Stomp.Headers.Subscribe.SELECTOR);
        if (selector != null) {
            rc = SelectorParser.parse(selector);
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
    }

    public String getCreatedTempDestinationName(ActiveMQDestination activeMQDestination) {
        // TODO Auto-generated method stub
        return null;
    }

    public ActiveMQDestination createTempQueue(String name) {
        // TODO Auto-generated method stub
        return null;
    }

    public ActiveMQDestination createTempTopic(String name) {
        // TODO Auto-generated method stub
        return null;
    }
    
    public MessageDelivery createMessageDelivery(MessageRecord record) {
        throw new UnsupportedOperationException();
    }
}
