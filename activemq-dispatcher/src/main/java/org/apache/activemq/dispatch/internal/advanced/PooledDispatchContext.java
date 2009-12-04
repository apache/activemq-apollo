package org.apache.activemq.dispatch.internal.advanced;

import org.apache.activemq.dispatch.DispatchObserver;

/**
 * A {@link PooledDispatchContext}s can be moved between different
 * dispatchers.
 */
public interface PooledDispatchContext extends DispatchContext {
    /**
     * Called to transfer a {@link PooledDispatchContext} to a new
     * Dispatcher.
     */
    public void setTargetQueue(Dispatcher newDispatcher);

    /**
     * Gets the dispatcher to which this PooledDispatchContext currently
     * belongs
     * 
     * @return
     */
    public Dispatcher getTargetQueue();

    /**
     * Gets the execution tracker for the context.
     * 
     * @return the execution tracker for the context:
     */
    public DispatchObserver getExecutionTracker();
}