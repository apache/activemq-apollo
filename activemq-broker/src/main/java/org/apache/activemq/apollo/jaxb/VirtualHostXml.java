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
package org.apache.activemq.apollo.jaxb;

import java.util.ArrayList;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementRef;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.adapters.XmlAdapter;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import org.apache.activemq.apollo.broker.VirtualHost;
import org.apache.activemq.util.buffer.AsciiBuffer;

@XmlRootElement(name = "virtual-host")
@XmlAccessorType(XmlAccessType.FIELD)
public class VirtualHostXml {
	
    @XmlJavaTypeAdapter(AsciiBufferAdapter.class)
    @XmlElement(name="host-name", required=true)
    private ArrayList<AsciiBuffer> hostNames = new ArrayList<AsciiBuffer>();

    @XmlElementRef   
    private StoreXml store;
    
	public VirtualHost createVirtualHost(BrokerXml brokerXml) throws Exception {
		VirtualHost rc = new VirtualHost();
		rc.setHostNames(hostNames);
		
		if( store != null ) {
			rc.setStore(store.createStore());
		}
		return rc;
	}

}
