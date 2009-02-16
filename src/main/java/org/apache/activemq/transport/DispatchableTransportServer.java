package org.apache.activemq.transport;

import org.apache.activemq.dispatch.IDispatcher;

public interface DispatchableTransportServer extends TransportServer {

	public void setDispatcher(IDispatcher dispatcher);
}
