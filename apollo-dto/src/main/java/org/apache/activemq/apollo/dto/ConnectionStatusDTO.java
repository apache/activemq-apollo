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



import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver;

import javax.xml.bind.annotation.*;

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@XmlRootElement(name="connection_status")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "connection_status_type")
@JsonTypeInfo(use=JsonTypeInfo.Id.CUSTOM, include=JsonTypeInfo.As.PROPERTY, property="@class")
@JsonTypeIdResolver(ApolloTypeIdResolver.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ConnectionStatusDTO extends ServiceStatusDTO {

    /**
     * The number of bytes that have been read from the connection.
     */
	@XmlAttribute(name="last_read_size")
	public long last_read_size;

    /**
     * The number of bytes that have been written to the connection.
     */
	@XmlAttribute(name="last_write_size")
	public long last_write_size;

    /**
     * The number of bytes that have been read from the connection.
     */
	@XmlAttribute(name="read_counter")
	public long read_counter;

    /**
     * The number of bytes that have been written to the connection.
     */
	@XmlAttribute(name="write_counter")
	public long write_counter;

    /**
     * The number of messages that have been sent to the connection.
     */
	@XmlAttribute(name="messages_sent")
	public long messages_sent;

    /**
     * The number of messages that have been received from the connection.
     */
	@XmlAttribute(name="messages_received")
	public long messages_received;

    /**
     * The connector that created the connection.
     */
	@XmlAttribute
	public String connector;

    /**
     * The protocol the connection is using.
     */
	@XmlAttribute
	public String protocol;

    /**
     * The version of the protocol being used.
     */
	@XmlAttribute(name="protocol_version")
	public String protocol_version;

    /**
     * The remote address of the connection
     */
	@XmlAttribute(name="remote_address")
	public String remote_address;

    /**
     * The local address of the connection
     */
	@XmlAttribute(name="local_address")
	public String local_address;

    /**
     * The session id of the protocol.
     */
	@XmlAttribute(name="protocol_session_id")
	public String protocol_session_id;

    /**
     * The connected user
     */
	@XmlAttribute
	public String user;

    /**
     * What the connection is currently waiting on
     */
    @XmlAttribute(name="waiting_on")
	public String waiting_on;

    /**
     * Opens subscriptions that the connection has created.
     */
    @XmlAttribute(name="subscription_count")
	public int subscription_count;

    /**
     * Holds detailed state data used to debug connections.
     */
	@XmlAttribute(name="debug")
	public String debug;

}
