package org.apache.activemq.dispatch;

import org.apache.activemq.dispatch.internal.advanced.AdvancedDispatchSPI;

/**
 * Handy interface to signal classes which would like an IDispatcher instance
 * injected into them.
 *  
 * @author chirino
 */
public interface DispatcherAware {

	public void setDispatcher(AdvancedDispatchSPI dispatcher);
	
}
