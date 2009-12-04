package org.apache.activemq.dispatch.internal.advanced;

/**
 * This interface is implemented by Dispatchable entities. A Dispatchable
 * entity registers with an {@link Dispatcher} and is returned a
 * {@link DispatchContext} which it can use to request the
 * {@link Dispatcher} to invoke {@link Dispatchable#dispatch()}
 * 
 * {@link Dispatcher} guarantees that {@link #dispatch()} will never invoke
 * dispatch concurrently unless the {@link Dispatchable} is registered with
 * more than one {@link Dispatcher};
 */
public interface Dispatchable {
    public boolean dispatch();
}