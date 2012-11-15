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
@XmlRootElement(name="queue_status")
@XmlAccessorType(XmlAccessType.FIELD)
@JsonIgnoreProperties(ignoreUnknown = true)
public class QueueStatusDTO extends ServiceStatusDTO {

    @XmlElement
    public QueueSettingsDTO config;

    @XmlElement
    public DestinationDTO binding;

    @XmlElement
    public DestMetricsDTO metrics = new DestMetricsDTO();

    /**
     * Status of the entries in the queue
     */
    @XmlElement(name="entry")
    public List<EntryStatusDTO> entries = new ArrayList<EntryStatusDTO>();

    /**
     * Ids of all connections that are producing to the destination
     */
    @XmlElement(name="producer")
    public List<LinkDTO> producers = new ArrayList<LinkDTO>();

    /**
     * Ids of all connections that are consuming from the destination
     */
    @XmlElement(name="consumer")
    public List<QueueConsumerLinkDTO> consumers = new ArrayList<QueueConsumerLinkDTO>();

    @XmlAttribute(name="max_enqueue_rate")
    public Integer max_enqueue_rate;
}