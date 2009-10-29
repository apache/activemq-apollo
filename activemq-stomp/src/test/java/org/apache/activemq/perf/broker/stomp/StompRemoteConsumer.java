package org.apache.activemq.perf.broker.stomp;

import java.io.IOException;
import java.util.HashMap;

import org.apache.activemq.apollo.broker.MessageDelivery;
import org.apache.activemq.apollo.broker.Router;
import org.apache.activemq.apollo.stomp.Stomp;
import org.apache.activemq.apollo.stomp.StompFrame;
import org.apache.activemq.apollo.stomp.StompMessageDelivery;
import org.apache.activemq.broker.RemoteConsumer;
import org.apache.activemq.flow.Flow;
import org.apache.activemq.flow.FlowController;
import org.apache.activemq.flow.IFlowResource;
import org.apache.activemq.flow.ISourceController;
import org.apache.activemq.flow.SizeLimiter;
import org.apache.activemq.flow.ISinkController.FlowControllable;
import org.apache.activemq.util.buffer.AsciiBuffer;

import static org.apache.activemq.util.buffer.AsciiBuffer.*;

public class StompRemoteConsumer extends RemoteConsumer {

    protected final Object inboundMutex = new Object();
    private FlowController<MessageDelivery> inboundController;
    private AsciiBuffer stompDestination;
    
    public StompRemoteConsumer() {
    }

    protected void setupSubscription() throws Exception, IOException {
        if( destination.getDomain().equals( Router.QUEUE_DOMAIN ) ) {
            stompDestination = ascii("/queue/"+destination.getName().toString());
        } else {
            stompDestination = ascii("/topic/"+destination.getName().toString());
        }
        
        StompFrame frame = new StompFrame(Stomp.Commands.CONNECT);
        transport.oneway(frame);
        
        HashMap<AsciiBuffer, AsciiBuffer> headers = new HashMap<AsciiBuffer, AsciiBuffer>();
        headers.put(Stomp.Headers.Subscribe.DESTINATION, stompDestination);
        headers.put(Stomp.Headers.Subscribe.ID, ascii("stomp-sub-"+name));
        headers.put(Stomp.Headers.Subscribe.ACK_MODE, Stomp.Headers.Subscribe.AckModeValues.AUTO);
        
        frame = new StompFrame(Stomp.Commands.SUBSCRIBE, headers);
        transport.oneway(frame);
        
    }
    
    protected void initialize() {
        // Setup the input processing..
        final Flow flow = new Flow("client-"+name+"-inbound", false);
        inputResumeThreshold = inputWindowSize/2;
        SizeLimiter<MessageDelivery> limiter = new SizeLimiter<MessageDelivery>(inputWindowSize, inputResumeThreshold);
        inboundController = new FlowController<MessageDelivery>(new FlowControllable<MessageDelivery>() {
            public void flowElemAccepted(ISourceController<MessageDelivery> controller, MessageDelivery elem) {
                messageReceived(controller, elem);
            }
            public String toString() {
                return flow.getFlowName();
            }
            public IFlowResource getFlowResource() {
                return null;
            }
        }, flow, limiter, inboundMutex);
        inboundController.setExecutor(getDispatcher().createPriorityExecutor(getDispatcher().getDispatchPriorities() - 1));
    }
    
    public void onCommand(Object command) {
        try {
            if (command.getClass() == StompFrame.class) {
                StompFrame frame = (StompFrame) command;
                if( Stomp.Responses.MESSAGE.equals(frame.getAction()) ) {
                    StompMessageDelivery md = new StompMessageDelivery(frame, getDestination());
                    while(!inboundController.offer(md, null) ) {
                        inboundController.waitForFlowUnblock();
                    }
                } else if( Stomp.Responses.CONNECTED.equals(frame.getAction()) ) {
                } else {
                    onException(new Exception("Unrecognized stomp command: " + frame.getAction()));
                }
            } else {
                onException(new Exception("Unrecognized command: " + command));
            }
        } catch (Exception e) {
            onException(e);
        }
    }
}