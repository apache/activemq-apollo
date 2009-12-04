package org.apache.activemq.dispatch.internal.advanced;

/**
 * Handy interface to signal classes which would like an IDispatcher instance
 * injected into them.
 *  
 * @author chirino
 */
public interface DispatcherAware {

	public void setDispatcher(Dispatcher dispatcher);
	
}
