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
@XmlRootElement(name="queue-status")
@XmlAccessorType(XmlAccessType.FIELD)
public class QueueStatusDTO extends LongIdDTO {

    /**
     * A unique id of the object within it's container
     */
    @JsonProperty
	@XmlAttribute
	public long id;

    @JsonProperty("enqueue_item_counter")
    @XmlAttribute(name="enqueue-item-counter")
    public long enqueueItemCounter;

    @JsonProperty("dequeue_item_counter")
    @XmlAttribute(name="dequeue-item-counter")
    public long dequeueItemCounter;

    @JsonProperty("enqueue_size_counter")
    @XmlAttribute(name="enqueue-size-counter")
    public long enqueueSizeCounter;

    @JsonProperty("dequeue_size_counter")
    @XmlAttribute(name="dequeue-size-counter")
    public long dequeueSizeCounter;

    @JsonProperty("nack_item_counter")
    @XmlAttribute(name="nack-item-counter")
    public long nackItemCounter;

    @JsonProperty("nack_size_counter")
    @XmlAttribute(name="nack-size-counter")
    public long nackSizeCounter;

    @JsonProperty("queue_size")
    @XmlAttribute(name="queue-size")
    public long queueSize;

    @JsonProperty("queue_items")
    @XmlAttribute(name="queue-items")
    public long queueItems;

    @JsonProperty("loading_size")
    @XmlAttribute(name="loading-size")
    public int loadingSize;

    @JsonProperty("flushing_size")
    @XmlAttribute(name="flushing-size")
    public int flushingSize;

    @JsonProperty("flushing_items")
    @XmlAttribute(name="flushed-items")
    public int flushedItems;

    @JsonProperty("capacity_used")
    @XmlAttribute(name="capacity-used")
    public int capacityUsed;

    @JsonProperty
    @XmlAttribute
    public int capacity;

    /**
     * Status of the entries in the queue
     */
    @JsonProperty("entries")
    @XmlElement(name="entry")
    public List<EntryStatusDTO> entries = new ArrayList<EntryStatusDTO>();


}