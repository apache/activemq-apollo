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
package org.apache.activemq.legacy.broker;

import java.net.URI;
import java.util.List;

import javax.management.MBeanServer;
import javax.net.ssl.KeyManager;
import javax.net.ssl.TrustManager;

import org.apache.activemq.Service;
import org.apache.activemq.legacy.broker.region.RegionBroker;
import org.apache.activemq.legacy.store.PersistenceAdapter;
import org.apache.activemq.legacy.usage.SystemUsage;
import org.apache.activemq.util.SslContext;

/**
 * @deprecated The entire 'org.apache.activemq.legacy' package will hopefully go away soon.
 */
@Deprecated
public class BrokerService implements Service {

    public static final String DEFAULT_PORT = "61616";
    public static final String DEFAULT_BROKER_NAME = "localhost";

	public URI getVmConnectorURI() {
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

    public <T> T getManagementContext() {
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

	public void addConnector(org.apache.activemq.legacy.broker.TransportConnector connector) {
		// TODO Auto-generated method stub
		
	}

	public List<TransportConnector> getTransportConnectors() {
		return null;
	}

	public void setDestinationPolicy(org.apache.activemq.legacy.broker.region.policy.PolicyMap policyMap) {
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

	public void setDeleteAllMessagesOnStartup(boolean b) {
		// TODO Auto-generated method stub
		
	}

	public PersistenceAdapter getPersistenceAdapter() {
		// TODO Auto-generated method stub
		return null;
	}

	public void removeConnector(TransportConnector connector) {
		// TODO Auto-generated method stub
		
	}

	public <T> T addNetworkConnector(String string) {
		// TODO Auto-generated method stub
		return null;
	}

	public <T> void removeNetworkConnector(T connector) {
		// TODO Auto-generated method stub
		
	}

	public void waitUntilStopped() {
		// TODO Auto-generated method stub
		
	}

	public MBeanServer getMBeanServer() {
		// TODO Auto-generated method stub
		return null;
	}

	public void setPopulateJMSXUserID(boolean b) {
		// TODO Auto-generated method stub
		
	}

	public void setBrokerName(String brokerName) {
		// TODO Auto-generated method stub
		
	}

	public void addConnector(URI tcpUri) {
		// TODO Auto-generated method stub
		
	}

	public TransportConnector addSslConnector(String bindLocation, KeyManager[] km, TrustManager[] tm, Object object) {
		// TODO Auto-generated method stub
		return null;
	}

	public void setSystemUsage(SystemUsage memoryManager) {
		// TODO Auto-generated method stub
		
	}

	public <T> void addNetworkConnector(T nc) {
		// TODO Auto-generated method stub
		
	}

	public void deleteAllMessages() {
		// TODO Auto-generated method stub
		
	}

	public void setPersistenceAdapter(PersistenceAdapter createPersistenceAdapter) {
		// TODO Auto-generated method stub
		
	}

	public void setDataDirectory(String string) {
		// TODO Auto-generated method stub
		
	}

	public void waitUntilStarted() {
		// TODO Auto-generated method stub
		
	}

	public void setUseShutdownHook(boolean b) {
		// TODO Auto-generated method stub
		
	}

	public void setWaitForSlave(boolean b) {
		// TODO Auto-generated method stub
		
	}

	public void setMasterConnectorURI(String masterUrl) {
		// TODO Auto-generated method stub
		
	}

	public void setAdvisorySupport(boolean b) {
		// TODO Auto-generated method stub
		
	}


}
