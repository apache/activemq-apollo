package org.apache.activemq.dispatch.internal.advanced;

import org.apache.activemq.dispatch.internal.advanced.LoadBalancer.ExecutionTracker;
import org.apache.activemq.dispatch.internal.advanced.Dispatcher.DispatchContext;

/**
 * A {@link PooledDispatchContext}s can be moved between different
 * dispatchers.
 */
public interface PooledDispatchContext extends DispatchContext {
    /**
     * Called to transfer a {@link PooledDispatchContext} to a new
     * Dispatcher.
     */
    public void assignToNewDispatcher(Dispatcher newDispatcher);

    /**
     * Gets the dispatcher to which this PooledDispatchContext currently
     * belongs
     * 
     * @return
     */
    public Dispatcher getDispatcher();

    /**
     * Gets the execution tracker for the context.
     * 
     * @return the execution tracker for the context:
     */
    public ExecutionTracker getExecutionTracker();
}