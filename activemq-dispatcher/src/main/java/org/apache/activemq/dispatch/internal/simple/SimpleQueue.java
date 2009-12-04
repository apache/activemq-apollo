package org.apache.activemq.dispatch.internal.simple;

import org.apache.activemq.dispatch.DispatchQueue;
import org.apache.activemq.dispatch.DispatchSystem.DispatchQueuePriority;

public interface SimpleQueue extends DispatchQueue {

    Runnable poll();
    DispatchQueuePriority getPriority();
}
