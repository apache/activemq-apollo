package org.apache.activemq.dispatch.internal.advanced;

import org.apache.activemq.dispatch.DispatchOption;
import org.apache.activemq.dispatch.DispatchQueue;
import org.apache.activemq.dispatch.internal.AbstractSerialDispatchQueue;

public class SerialDispatchQueue extends AbstractSerialDispatchQueue {

    public SerialDispatchQueue(String label, DispatchOption...options) {
        super(label, options);
//        context = new DispatchContext(this, true, label);
}
    
//    @Override
//    protected void dispatchSelfAsync() {
//        AdvancedQueue aq = ((AdvancedQueue)targetQueue);
//        super.dispatchSelfAsync();
//    }
    
    @Override
    public void run() {
        DispatchQueue original = AdvancedDispatcher.CURRENT_QUEUE.get();
        AdvancedDispatcher.CURRENT_QUEUE.set(this);
        try {
            super.run();
        } finally {
            AdvancedDispatcher.CURRENT_QUEUE.set(original);
        }
    }
    

}
