package org.apache.activemq.dispatch;


/**
 * Handy interface to signal classes which would like an Dispatcher instance
 * injected into them.
 *  
 * @author chirino
 */
public interface DispatcherAware {

	public void setDispatcher(Dispatcher dispatcher);
	
}
