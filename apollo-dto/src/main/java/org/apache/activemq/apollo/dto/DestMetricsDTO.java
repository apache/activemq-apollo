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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * <p>
 *     Collects metrics about the status of a destination since the
 *     time the broker was started.
 * </p>
 *
 * <p>
 *     Note: To compute the total size in bytes of messages that are in persistent
 *     storage use the following algorithm:
 *
 *     swapped_out_size = queue_size - swapped_in_size
 *
 * </p>
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@XmlRootElement(name="dest_metrics")
@XmlAccessorType(XmlAccessType.FIELD)
@JsonIgnoreProperties(ignoreUnknown = true)
public class DestMetricsDTO {
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
     * The time stamp of when the last dequeue to consumers occurred.
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
     * The current number of consumers consuming from the destination.
     */
    @XmlAttribute(name="consumer_count")
    public long consumer_count;

    /**
     * The number of messages which expired before they could be processed.
     */
    @XmlAttribute(name="expired_item_counter")
    public long expired_item_counter;

    /**
     * The total size in bytes of messages which expired before
     * they could be processed.
     */
    @XmlAttribute(name="expired_size_counter")
    public long expired_size_counter;

    /**
     * The time stamp of when the last message expiration occurred.
     */
    @XmlAttribute(name="expired_ts")
    public long expired_ts;

    /**
     * The number of messages that were delivered to
     * a consumer but which the consumer did not successfully process.
     */
    @XmlAttribute(name="nack_item_counter")
    public long nack_item_counter;

    /**
     * The total size in bytes of messages that were delivered to
     * a consumer but which the consumer did not successfully process.
     */
    @XmlAttribute(name="nack_size_counter")
    public long nack_size_counter;

    /**
     * The time stamp of when the last nack occurred.
     */
    @XmlAttribute(name="nack_ts")
    public long nack_ts;

    /**
     * The total size in bytes of messages that are sitting in the queue.
     */
    @XmlAttribute(name="queue_size")
    public long queue_size;

    /**
     * The total number of messages that are sitting in the queue.
     */
    @XmlAttribute(name="queue_items")
    public long queue_items;

    /**
     * The maximum amount of RAM this queue will use to process in
     * flight messages.  The queue will either flow control producers
     * or swap messages to persistent storage once this limit is reached.
     */
    @XmlAttribute(name="swapped_in_size_max")
    public long swapped_in_size_max;

    /**
     * The total size in bytes of messages that are resident in
     * the broker's RAM.
     */
    @XmlAttribute(name="swapped_in_size")
    public long swapped_in_size;

    /**
     * The total number of messages that are resident in
     * the broker's RAM.
     */
    @XmlAttribute(name="swapped_in_items")
    public long swapped_in_items;

    /**
     * The total size in bytes of messages that are being
     * loaded from persistent storage into RAM.
     */
    @XmlAttribute(name="swapping_in_size")
    public long swapping_in_size;

    /**
     * The total size in bytes of messages that are being
     * evicted from RAM into persistent storage.
     */
    @XmlAttribute(name="swapping_out_size")
    public long swapping_out_size;

    /**
     * The total number of messages that have ever been
     * moved from RAM into persistent storage.
     */
    @XmlAttribute(name="swap_out_item_counter")
    public long swap_out_item_counter;

    /**
     * The total size in bytes of messages that have ever been
     * moved from RAM into persistent storage.
     */
    @XmlAttribute(name="swap_out_size_counter")
    public long swap_out_size_counter;

    /**
     * The total number of messages that have ever been
     * moved from persistent storage into RAM.
     */
    @XmlAttribute(name="swap_in_item_counter")
    public long swap_in_item_counter;

    /**
     * The total size in bytes of messages that have ever been
     * moved from persistent storage into RAM.
     */
    @XmlAttribute(name="swap_in_size_counter")
    public long swap_in_size_counter;

}
