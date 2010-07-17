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

import javax.xml.bind.annotation.*;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@XmlRootElement(name = "virtual-host")
@XmlAccessorType(XmlAccessType.FIELD)
public class VirtualHostDTO extends ServiceDTO<String> {

    @XmlElement(name="host-name", required=true)
    public ArrayList<String> host_names = new ArrayList<String>();

    @XmlElementRef
    public StoreDTO store;

    /**
     * Should queues be auto created when they are first accessed
     * by clients?
     */
    @JsonProperty("auto_create_queues")
    @XmlAttribute(name="auto-create-queues")
    public boolean auto_create_queues = true;

    /**
     * Should queues be purged on startup?
     */
    @XmlAttribute(name="purge-on-startup")
    public boolean purge_on_startup = false;

    /**
     * Holds the configuration for the destinations.
     */
    @XmlElement(name="destination")
    public ArrayList<DestinationDTO> destinations = new ArrayList<DestinationDTO>();


}
