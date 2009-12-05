package org.apache.activemq.dispatch.internal.advanced;

import org.apache.activemq.dispatch.internal.SerialDispatchQueue;

public class AdvancedSerialDispatchQueue extends SerialDispatchQueue {

    
    public AdvancedSerialDispatchQueue(String label) {
        super(label);
//        context = new DispatchContext(this, true, label);
}
    
//    @Override
//    protected void dispatchSelfAsync() {
//        AdvancedQueue aq = ((AdvancedQueue)targetQueue);
//        super.dispatchSelfAsync();
//    }

}
