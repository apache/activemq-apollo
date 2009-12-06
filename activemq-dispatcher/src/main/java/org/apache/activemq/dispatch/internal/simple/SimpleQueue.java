package org.apache.activemq.dispatch.internal.simple;

import org.apache.activemq.dispatch.DispatchQueue;
import org.apache.activemq.dispatch.DispatchPriority;

public interface SimpleQueue extends DispatchQueue {

    Runnable poll();
    DispatchPriority getPriority();
}
