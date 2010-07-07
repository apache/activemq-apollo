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
@XmlRootElement(name="destination-status")
@XmlAccessorType(XmlAccessType.FIELD)
public class EntryStatusDTO extends IdDTO<Long> {

    /**
     * A unique id of the object within it's container
     */
	@XmlAttribute(name="enqueue-item-counter")
	public long id;

    @XmlAttribute(name="enqueue-item-counter")
    public long enqueueItemCounter;
    @XmlAttribute(name="dequeue-item-counter")
    public long dequeueItemCounter;
    @XmlAttribute(name="enqueue-size-counter")
    public long enqueueSizeCounter;
    @XmlAttribute(name="dequeue-size-counter")
    public long dequeueSizeCounter;
    @XmlAttribute(name="nack-item-counter")
    public long nackItemCounter;
    @XmlAttribute(name="nack-size-counter")
    public long nackSizeCounter;

    @XmlAttribute(name="queue-size")
    public long queueSize;
    @XmlAttribute(name="queue-items")
    public long queueItems;

    @XmlAttribute(name="loading-size")
    public int loadingSize;
    @XmlAttribute(name="flushing-size")
    public int flushingSize;
    @XmlAttribute(name="flushed-items")
    public int flushedItems;

    @XmlAttribute(name="capacity")
    public int capacity;

}