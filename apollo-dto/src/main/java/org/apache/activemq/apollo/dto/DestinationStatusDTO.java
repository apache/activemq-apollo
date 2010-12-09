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

import javax.xml.bind.annotation.*;
import java.util.ArrayList;
import java.util.List;

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@XmlRootElement(name="destination-status")
@XmlAccessorType(XmlAccessType.FIELD)
public class DestinationStatusDTO extends LongIdDTO {

    /**
     * The destination name
     */
    @XmlAttribute
    public String name;

    @XmlElement
    public DestinationDTO config;

    /**
     * Ids of all connections that are producing to the destination
     */
    @XmlElement(name="producer")
    @JsonProperty("producers")
    public List<LinkDTO> producers = new ArrayList<LinkDTO>();

    /**
     * Ids of all connections that are consuming from the destination
     */
    @XmlElement(name="consumer")
    @JsonProperty("consumers")
    public List<LinkDTO> consumers = new ArrayList<LinkDTO>();

    /**
     * Ids of all queues that are associated with the destination
     */
    @XmlElement(name="queue")
    @JsonProperty("queues")
    public List<LongIdLabeledDTO> queues = new ArrayList<LongIdLabeledDTO>();
}