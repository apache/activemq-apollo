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
@XmlRootElement(name="queue_metrics")
@XmlAccessorType(XmlAccessType.FIELD)
public class QueueMetricsDTO {

    @XmlAttribute(name="enqueue_item_counter")
    public long enqueue_item_counter;

    @XmlAttribute(name="enqueue_size_counter")
    public long enqueue_size_counter;

    @XmlAttribute(name="enqueue_ts")
    public long enqueue_ts;

    @XmlAttribute(name="dequeue_item_counter")
    public long dequeue_item_counter;

    @XmlAttribute(name="dequeue_size_counter")
    public long dequeue_size_counter;

    @XmlAttribute(name="dequeue_ts")
    public long dequeue_ts;

    @XmlAttribute(name="nack_item_counter")
    public long nack_item_counter;

    @XmlAttribute(name="nack_size_counter")
    public long nack_size_counter;

    @XmlAttribute(name="nack_ts")
    public long nack_ts;

    @XmlAttribute(name="queue_size")
    public long queue_size;

    @XmlAttribute(name="queue_items")
    public long queue_items;

    @XmlAttribute(name="swapped_in_size")
    public int swapped_in_size;

    @XmlAttribute(name="swapped_in_items")
    public int swapped_in_items;

    @XmlAttribute(name="swapping_in_size")
    public int swapping_in_size;

    @XmlAttribute(name="swapping_out_size")
    public int swapping_out_size;

    @XmlAttribute(name="swapped_in_size_max")
    public int swapped_in_size_max;

    @XmlAttribute(name="swap_out_item_counter")
    public long swap_out_item_counter;

    @XmlAttribute(name="swap_out_size_counter")
    public long swap_out_size_counter;

    @XmlAttribute(name="swap_in_item_counter")
    public long swap_in_item_counter;

    @XmlAttribute(name="swap_in_size_counter")
    public long swap_in_size_counter;
}
