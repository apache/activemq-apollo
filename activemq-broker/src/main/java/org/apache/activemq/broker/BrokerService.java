/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.broker;

import java.net.URI;
import java.util.List;

import org.apache.activemq.Service;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.usage.SystemUsage;

public class BrokerService implements Service {

    public static final String DEFAULT_PORT = "61616";
    public static final String DEFAULT_BROKER_NAME = "localhost";

	public Object getVmConnectorURI() {
        // TODO Auto-generated method stub
        return null;
    }

    public RegionBroker getRegionBroker() {
        // TODO Auto-generated method stub
        return null;
    }

    public boolean isUseJmx() {
        // TODO Auto-generated method stub
        return false;
    }

    public ManagementContext getManagementContext() {
        // TODO Auto-generated method stub
        return null;
    }

    public <T> T addNetworkConnector(URI connectorURI) {
        // TODO Auto-generated method stub
        return null;
    }

    public String getBrokerName() {
        // TODO Auto-generated method stub
        return null;
    }

    public Broker getBroker() {
        return null;
    }

    public SslContext getSslContext() {
        // TODO Auto-generated method stub
        return null;
    }

	public void start() throws Exception {
		// TODO Auto-generated method stub
		
	}

	public void stop() throws Exception {
		// TODO Auto-generated method stub
		
	}

	public void setPersistent(boolean b) {
		// TODO Auto-generated method stub
		
	}

	public TransportConnector addConnector(String uri) {
		// TODO Auto-generated method stub
		return null;
	}

	public void setUseJmx(boolean b) {
		// TODO Auto-generated method stub
		
	}

	public <T> void setDestinations(T[] activeMQDestinations) {
		// TODO Auto-generated method stub
		
	}

	public void addConnector(org.apache.activemq.broker.TransportConnector connector) {
		// TODO Auto-generated method stub
		
	}

	public List<TransportConnector> getTransportConnectors() {
		return null;
	}

	public void setDestinationPolicy(org.apache.activemq.broker.region.policy.PolicyMap policyMap) {
		// TODO Auto-generated method stub
		
	}

	public SystemUsage getSystemUsage() {
		// TODO Auto-generated method stub
		return null;
	}

	public <T> T getAdminView() {
		// TODO Auto-generated method stub
		return null;
	}

    public void addShutdownHook(Runnable runnable) {
		// TODO Auto-generated method stub
		
	}

	public boolean isStarted() {
		// TODO Auto-generated method stub
		return false;
	}


}
