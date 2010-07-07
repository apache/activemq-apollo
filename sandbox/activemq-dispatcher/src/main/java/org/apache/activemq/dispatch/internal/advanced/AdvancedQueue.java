package org.apache.activemq.dispatch.internal.advanced;

import org.apache.activemq.dispatch.internal.simple.GlobalDispatchQueue;
import org.apache.activemq.dispatch.internal.simple.SerialDispatchQueue;
import org.apache.activemq.dispatch.internal.simple.ThreadDispatchQueue;

public interface AdvancedQueue {

    SerialDispatchQueue isSerialDispatchQueue();
    ThreadDispatchQueue isThreadDispatchQueue();
    GlobalDispatchQueue isGlobalDispatchQueue();
}
