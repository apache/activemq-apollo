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
package org.apache.activemq.apollo.dto;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@XmlRootElement(name="stomp-connection-status")
@XmlAccessorType(XmlAccessType.FIELD)
public class StompConnectionStatusDTO extends ConnectionStatusDTO {

    /**
     * The version of the STOMP protocol being used.
     */
	@XmlAttribute(name="protocol-version")
	public String protocol_version;

    /**
     * The connected user
     */
	@XmlAttribute
	public String user;

    /**
     * What the connection is currently waiting on
     */
    @XmlAttribute(name="waiting-on")
	public String waiting_on;

    /**
     * Opens subscriptions that the connection has created.
     */
    @XmlAttribute(name="subscription-count")
	public int subscription_count;


}
