package org.apache.activemq.broker.openwire.stomp;

import java.io.IOException;
import java.util.HashMap;

import org.apache.activemq.broker.MessageDelivery;
import org.apache.activemq.broker.RemoteProducer;
import org.apache.activemq.broker.Router;
import org.apache.activemq.broker.stomp.StompMessageDelivery;
import org.apache.activemq.flow.Flow;
import org.apache.activemq.flow.ISourceController;
import org.apache.activemq.flow.SizeLimiter;
import org.apache.activemq.queue.QueueDispatchTarget;
import org.apache.activemq.queue.SingleFlowRelay;
import org.apache.activemq.transport.stomp.Stomp;
import org.apache.activemq.transport.stomp.StompFrame;

public class StompRemoteProducer extends RemoteProducer {

    private String stompDestination;

    StompRemoteProducer() {
    }
    
    protected void setupProducer() throws Exception, IOException {
        if( destination.getDomain().equals( Router.QUEUE_DOMAIN ) ) {
            stompDestination = "/queue/"+destination.getName().toString();
        } else {
            stompDestination = "/topic/"+destination.getName().toString();
        }
        
        StompFrame frame = new StompFrame(Stomp.Commands.CONNECT);
        transport.oneway(frame);
        
    }
    
    protected void initialize() {
        Flow flow = new Flow("client-"+name+"-outbound", false);
        outputResumeThreshold = outputWindowSize/2;
        SizeLimiter<MessageDelivery> outboundLimiter = new SizeLimiter<MessageDelivery>(outputWindowSize, outputResumeThreshold);
        SingleFlowRelay<MessageDelivery> outboundQueue = new SingleFlowRelay<MessageDelivery>(flow, flow.getFlowName(), outboundLimiter);
        this.outboundQueue = outboundQueue;
        
        outboundController = outboundQueue.getFlowController(flow);
        outboundQueue.setDrain(new QueueDispatchTarget<MessageDelivery>() {
            public void drain(final MessageDelivery message, final ISourceController<MessageDelivery> controller) {
                StompFrame msg = message.asType(StompFrame.class);
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

        HashMap<String, String> headers = new HashMap<String, String>(5);
        headers.put(Stomp.Headers.Send.DESTINATION, stompDestination);
        
        if (property != null) {
            headers.put(property, property);
        }
        
        byte[] content = toContent(createPayload());
        
        headers.put(Stomp.Headers.CONTENT_LENGTH, ""+content.length);
        
        StompFrame fram = new StompFrame(Stomp.Commands.SEND, headers, content);
        next = new StompMessageDelivery(fram, getDestination());
    }

    private byte[] toContent(String data) {
        byte rc[] = new byte[data.length()];
        char[] chars = data.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            rc[i] = (byte)(chars[i] & 0xFF);
        }
        return rc;
    }
}

