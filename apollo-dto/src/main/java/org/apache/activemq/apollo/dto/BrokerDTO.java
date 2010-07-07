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

import org.codehaus.jackson.annotate.JsonProperty;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@XmlRootElement(name="broker")
@XmlAccessorType(XmlAccessType.FIELD)
public class BrokerDTO extends ServiceDTO<String> {

    /**
     * Used to track config revisions.
     */
    @JsonProperty
    @XmlAttribute
    public int rev;

    /**
     * Used to track who last modified the configuration.
     */
    @XmlAttribute(name="modified-by")
    public String modified_by;

    /**
     * Used to store any configuration notes.
     */
    @JsonProperty
    @XmlElement
    public String notes;

    /**
     * A broker can service many virtual hosts.
     */
    @XmlElement(name="virtual-host")
    public List<VirtualHostDTO> virtual_hosts = new ArrayList<VirtualHostDTO>();

    /**
     * A broker accepts connections via it's configured connectors.
     */
    @JsonProperty
    @XmlElement
    public List<ConnectorDTO> connectors = new ArrayList<ConnectorDTO>();

    /**
     * The base data directory of the broker.  It will store
     * persistent data under it. 
     */
    @JsonProperty
    @XmlAttribute
    public String basedir;

    

}
