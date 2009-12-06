package org.apache.activemq.dispatch;


/**
 * Handy interface to signal classes which would like an DispatchSPI instance
 * injected into them.
 *  
 * @author chirino
 */
public interface DispatchAware {

	public void setDispatcher(Dispatch dispatcher);
	
}
