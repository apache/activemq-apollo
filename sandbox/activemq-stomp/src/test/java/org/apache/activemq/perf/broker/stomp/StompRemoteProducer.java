package org.apache.activemq.perf.broker.stomp;

import java.io.IOException;
import java.util.HashMap;

import org.apache.activemq.apollo.broker.MessageDelivery;
import org.apache.activemq.apollo.broker.Router;
import org.apache.activemq.apollo.stomp.Stomp;
import org.apache.activemq.apollo.stomp.StompFrame;
import org.apache.activemq.apollo.stomp.StompMessageDelivery;
import org.apache.activemq.broker.RemoteProducer;
import org.apache.activemq.flow.Flow;
import org.apache.activemq.flow.ISourceController;
import org.apache.activemq.flow.SizeLimiter;
import org.apache.activemq.queue.QueueDispatchTarget;
import org.apache.activemq.queue.SingleFlowRelay;
import org.apache.activemq.util.buffer.AsciiBuffer;

import static org.apache.activemq.util.buffer.AsciiBuffer.*;

public class StompRemoteProducer extends RemoteProducer {

    private AsciiBuffer stompDestination;
    private AsciiBuffer property;
    
    StompRemoteProducer() {
    }
    
    protected void setupProducer() throws Exception, IOException {
        if( destination.getDomain().equals( Router.QUEUE_DOMAIN ) ) {
            stompDestination = ascii("/queue/"+destination.getName().toString());
        } else {
            stompDestination = ascii("/topic/"+destination.getName().toString());
        }
        
        StompFrame frame = new StompFrame(Stomp.Commands.CONNECT);
        transport.oneway(frame);
        
    }
    
    protected void initialize() {
        
        property = ascii(super.property);
        Flow flow = new Flow("client-"+name+"-outbound", false);
        outputResumeThreshold = outputWindowSize/2;
        SizeLimiter<MessageDelivery> outboundLimiter = new SizeLimiter<MessageDelivery>(outputWindowSize, outputResumeThreshold) { 
            public int getElementSize(MessageDelivery elem) {
                StompMessageDelivery md = (StompMessageDelivery) elem;
                return md.getStompFrame().getContent().length;
            }
        };
        SingleFlowRelay<MessageDelivery> outboundQueue = new SingleFlowRelay<MessageDelivery>(flow, flow.getFlowName(), outboundLimiter);
        this.outboundQueue = outboundQueue;
        
        outboundController = outboundQueue.getFlowController(flow);
        outboundQueue.setDrain(new QueueDispatchTarget<MessageDelivery>() {
            public void drain(final MessageDelivery message, final ISourceController<MessageDelivery> controller) {
                StompMessageDelivery md = (StompMessageDelivery) message;
                StompFrame msg = md.getStompFrame();
                write(msg, new Runnable(){
                    public void run() {
                        controller.elementDispatched(message);
                    }
                });
            }
        });
    }
    
    public void onCommand(Object command) {
        try {
            if (command.getClass() == StompFrame.class) {
                StompFrame frame = (StompFrame) command;
                if( Stomp.Responses.CONNECTED.equals(frame.getAction()) ) {
                    
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
    
    protected void createNextMessage() {
        int priority = this.priority;
        if (priorityMod > 0) {
            priority = counter % priorityMod == 0 ? 0 : priority;
        }

        HashMap<AsciiBuffer, AsciiBuffer> headers = new HashMap<AsciiBuffer, AsciiBuffer>(4);
        headers.put(Stomp.Headers.Send.DESTINATION, stompDestination);
        
        if (property != null) {
            headers.put(property, property);
        }
        
        AsciiBuffer content = ascii(createPayload());
        
//        headers.put(Stomp.Headers.CONTENT_LENGTH, ascii(Integer.toString(content.length)));
        
        StompFrame frame = new StompFrame(Stomp.Commands.SEND, headers, content);
        next = new StompMessageDelivery(frame, getDestination());
    }

}

