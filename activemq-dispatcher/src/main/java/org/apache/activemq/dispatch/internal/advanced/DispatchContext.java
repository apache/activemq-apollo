package org.apache.activemq.dispatch.internal.advanced;

import java.util.concurrent.RejectedExecutionException;

import org.apache.activemq.dispatch.DispatchObserver;


/**
 * Returned to callers registered with this dispathcer. Used by the caller
 * to inform the dispatcher that it is ready for dispatch.
 * 
 * Note that DispatchContext is not safe for concurrent access by multiple
 * threads.
 */
public interface DispatchContext {
    
    /**
     * Once registered with a dispatcher, this can be called to request
     * dispatch. The {@link Dispatchable} will remain in the dispatch queue
     * until a subsequent call to {@link Dispatchable#dispatch()} returns
     * false;
     * 
     * @throws RejectedExecutionException If the dispatcher has been shutdown.
     */
    public void requestDispatch() throws RejectedExecutionException;

    /**
     * This can be called to update the dispatch priority.
     * 
     * @param priority
     */
    public void updatePriority(int priority);

    /**
     * Gets the name of the dispatch context
     * 
     * @return The dispatchable
     */
    public String getName();

    /**
     * This must be called to release any resource the dispatcher is holding
     * on behalf of this context. Once called this {@link DispatchContext} should
     * no longer be used. 
     */
    public void close(boolean sync);
    
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