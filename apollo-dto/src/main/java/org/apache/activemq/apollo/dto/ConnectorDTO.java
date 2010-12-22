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

import javax.xml.bind.annotation.*;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * 
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@XmlRootElement(name = "connector")
@XmlAccessorType(XmlAccessType.FIELD)
public class ConnectorDTO extends ServiceDTO<String> {

    /**
     * The transport uri which it will accept connections on.
     */
    @XmlAttribute
    public String bind;

    /**
     * The protocol that the transport will use.
     */
    @XmlAttribute
    public String protocol;

    /**
     * The uri which will be advertised for remote endpoints to connect to.
     */
    @XmlAttribute
    public String advertise;

    @XmlAttribute(name="connection_limit")
    public Integer connection_limit;

    /**
     * A broker accepts connections via it's configured connectors.
     */
    @XmlElementRef
    public List<ProtocolDTO> protocols = new ArrayList<ProtocolDTO>();

    @XmlElement(name="acl")
    public ConnectorAclDTO acl;

}