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
import org.codehaus.jackson.annotate.JsonTypeInfo;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlSeeAlso;
import javax.xml.bind.annotation.XmlType;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@XmlType(name = "store-status-type")
@XmlSeeAlso({HawtDBStoreStatusDTO.class})
@JsonTypeInfo(use=JsonTypeInfo.Id.CLASS, include=JsonTypeInfo.As.PROPERTY, property="@class")
public class StoreStatusDTO {

    /**
     * The state of the service.
     */
	@XmlAttribute
	public String state;

    /**
     * Since when has the service been in in this state?  In milliseconds since the epoch. 
     */
	@XmlAttribute(name="state-since")
	public long state_since;

    /**
     * The number of message stores that were canceled before they were flushed.
     */
    @XmlAttribute(name="canceled-message-counter")
    public long canceled_message_counter;

    /**
     * The number of message stores that were flushed.
     */
    @XmlAttribute(name="flushed-message-counter")
    public long flushed_message_counter;

    /**
     * The number of enqueues that were canceled before they were flushed.
     */
    @XmlAttribute(name="canceled-enqueue-counter")
    public long canceled_enqueue_counter;

    /**
     * The number of enqueues that were flushed.
     */
    @XmlAttribute(name="flushed-enqueue-counter")
    public long flushed_enqueue_counter;

    /**
     * The amount of time it takes to load a message from the store.
     */
    @XmlElement(name="message-load-latency")
    public TimeMetricDTO message_load_latency;

    /**
     * The amount of time it takes to flush a unit of work to the store
     */
    @XmlElement(name="flush-latency")
    public TimeMetricDTO flush_latency;

}