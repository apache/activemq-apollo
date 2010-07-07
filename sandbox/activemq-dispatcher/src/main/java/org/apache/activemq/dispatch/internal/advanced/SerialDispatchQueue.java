package org.apache.activemq.dispatch.internal.advanced;

import java.util.concurrent.TimeUnit;

import org.apache.activemq.dispatch.DispatchOption;
import org.apache.activemq.dispatch.DispatchQueue;
import org.apache.activemq.dispatch.internal.AbstractSerialDispatchQueue;

public class SerialDispatchQueue extends AbstractSerialDispatchQueue {

    AdvancedDispatcher dispather;
    public SerialDispatchQueue(AdvancedDispatcher dispather, String label, DispatchOption...options) {
        super(label, options);
        this.dispather = dispather;
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
    
    public void dispatchAfter(Runnable runnable, long delay, TimeUnit unit) {
        dispather.schedule(runnable, delay, unit);
    }
}
