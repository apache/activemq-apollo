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

/**
 * <p>
 *     Collects metrics about the status of a destination since the
 *     time a broker gets started.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class DestinationMetricsDTO {

    /**
     * The current time on the broker machine.  In milliseconds since the epoch.
     */
	@XmlAttribute(name="current_time")
	public long current_time;

    /**
     * The number of messages that have been sent to the destination.
     */
    @XmlAttribute(name="enqueue_item_counter")
    public long enqueue_item_counter;

    /**
     * The total size in bytes of messages that have been sent
     * to the destination
     */
    @XmlAttribute(name="enqueue_size_counter")
    public long enqueue_size_counter;

    /**
     * The time stamp of when the last message was sent to the destination.
     */
    @XmlAttribute(name="enqueue_ts")
    public long enqueue_ts;

    /**
     * The number of messages that have been sent to consumers on
     * the destination.
     */
    @XmlAttribute(name="dequeue_item_counter")
    public long dequeue_item_counter;

    /**
     * The total size in bytes of messages that have been sent to consumers on
     * the destination.
     */
    @XmlAttribute(name="dequeue_size_counter")
    public long dequeue_size_counter;

    /**
     * The time stamp of when the last dequeue to a consumers occurred.
     */
    @XmlAttribute(name="dequeue_ts")
    public long dequeue_ts;

    /**
     * The total number of producers that have ever sent to
     * the destination.
     */
    @XmlAttribute(name="producer_counter")
    public long producer_counter;

    /**
     * The total number of consumers that have ever subscribed to
     * the queue.
     */
    @XmlAttribute(name="consumer_counter")
    public long consumer_counter;


    /**
     * The current number of producers sending to the destination
     * the queue.
     */
    @XmlAttribute(name="producer_count")
    public long producer_count;

    /**
     * The current number of producers consuming from the destination.
     */
    @XmlAttribute(name="consumer_count")
    public long consumer_count;

}
