package org.apache.activemq.transport;

import org.apache.activemq.dispatch.IDispatcher;

public interface DispatchableTransport extends Transport{

	public void setDispatcher(IDispatcher dispatcher);
	public void setName(String name);
}
