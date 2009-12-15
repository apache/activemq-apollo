package org.apache.activemq.dispatch.internal.simple;

import org.apache.activemq.dispatch.DispatchQueue;
import org.apache.activemq.dispatch.DispatchPriority;

public interface SimpleQueue extends DispatchQueue {

    DispatchPriority getPriority();
    
    SerialDispatchQueue isSerialDispatchQueue();
    ThreadDispatchQueue isThreadDispatchQueue();
    GlobalDispatchQueue isGlobalDispatchQueue();
    
    SimpleQueue getTargetQueue();

}
