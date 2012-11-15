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

import javax.xml.bind.annotation.*;
import java.util.ArrayList;
import java.util.List;

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@XmlRootElement(name="connector_status")
@XmlAccessorType(XmlAccessType.FIELD)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ConnectorStatusDTO extends ServiceStatusDTO {

    /**
     * The local address the connector has bound
     */
    @XmlAttribute(name="local_address")
    public String local_address;

    @XmlAttribute(name="protocol")
    public String protocol;

    /**
     * The number of connections that this connector has accepted.
     */
    @XmlAttribute(name="connection_counter")
    public long connection_counter;

    /**
     * The number of connections that this connector has currently connected.
     */
    @XmlAttribute
    public long connected;

    /**
     * The connections that have been created via the connector.
     */
    @XmlElement(name="connection")
    public List<LongIdLabeledDTO> connections = new ArrayList<LongIdLabeledDTO>();

    /**
     * The number of messages that have been sent to connections created
     * by this connector.
     */
	@XmlAttribute(name="messages_sent")
	public long messages_sent;

    /**
     * The number of messages that have been received from connections created
     * by this connector.
     */
	@XmlAttribute(name="messages_received")
	public long messages_received;

    /**
     * The number of bytes that have been read from the connections created by this
     * connector.
     */
	@XmlAttribute(name="read_counter")
	public long read_counter;

    /**
     * The number of bytes that have been written to the connections created by this
     * connector.
     */
	@XmlAttribute(name="write_counter")
	public long write_counter;

}
