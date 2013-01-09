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
import javax.xml.bind.annotation.XmlAnyElement;
import javax.xml.bind.annotation.XmlAttribute;
import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@XmlAccessorType(XmlAccessType.FIELD)
@JsonIgnoreProperties(ignoreUnknown = true)
public class QueueSettingsDTO {

    /**
     *  The amount of memory buffer space to use for swapping messages
     *  out.
     */
    @XmlAttribute(name="tail_buffer")
    public String tail_buffer;

    /**
     * Should this queue persistently store it's entries?
     */
    @XmlAttribute(name="persistent")
    public Boolean persistent;

    /**
     * Should the destination dispatch messages to consumers
     * using round robin distribution strategy?  Defaults to true.
     * If set to false, then messages will be dispatched
     * to the first attached consumers until they throttle the broker.
     */
    @XmlAttribute(name="round_robin")
    public Boolean round_robin;

    /**
     * When set to true, the queue
     * will drain the required message group consumers of messages before
     * allowing new messages to dispatched to messages groups which have been
     * moved to a different consumer due to re-balancing. Defaults to true.
     */
    @XmlAttribute(name="message_group_graceful_handoff")
    public Boolean message_group_graceful_handoff;

    /**
     * Should messages be swapped out of memory if
     * no consumers need the message?
     */
    @XmlAttribute(name="swap")
    public Boolean swap;

    /**
     * The number max number of swapped queue entries to load
     * from the store at a time.  Note that swapped entries are just
     * reference pointers to the actual messages.  When not loaded,
     * the batch is referenced as sequence range to conserve memory.
     */
    @XmlAttribute(name="swap_range_size")
    public Integer swap_range_size;

    /**
     * The maximum amount of size the queue is allowed
     * to grow to.  If not set then there is no limit.  You can
     * use settings values like: 500mb or 1g just plain 1024000
     */
    @XmlAttribute(name="quota")
    public String quota;

    /**
     * The maximum number of messages queue is allowed
     * to grow to.  If not set then there is no limit.
     */
    @XmlAttribute(name="quota_messages")
    public Long quota_messages;

    /**
     * Once the queue is full, the `full_policy` controls how the
     * queue behaves when additional messages attempt to be enqueued
     * onto the queue.
     *
     * You can set it to one of the following options:
     *  `block`: The producer blocks until some space frees up.
     *  `drop tail`: Drops new messages being enqueued on the queue.
     *  `drop head`: Drops old messages at the front of the queue.
     *
     * If the queue is persistent then it is considered full when the max
     * quota size is reached.  If the queue is not persistent then
     * the queue is considered full once it's `tail_buffer` fills up.
     * Defaults to 'block' if not specified.
     */
    @XmlAttribute(name="full_policy")
    public String full_policy;

    /**
     *  The message delivery rate (in bytes/sec) at which
     *  the queue considers the consumers fast and
     *  may start slowing down producers to match the consumption
     *  rate if the consumers are at the tail of the queue.
     */
    @XmlAttribute(name="fast_delivery_rate")
    public String fast_delivery_rate;

    /**
     * If set, and the the current delivery
     * rate is exceeding the configured value
     * of fast_delivery_rate and the consumers
     * are spending more time loading from
     * the store than delivering, then the
     * enqueue rate will be throttled to the
     * specified value so that the consumers
     * can catch up and reach the tail of the queue.
     */
    @XmlAttribute(name="catchup_enqueue_rate")
    public String catchup_enqueue_rate;

    /**
     *  The maximum enqueue rate of the queue
     */
    @XmlAttribute(name="max_enqueue_rate")
    public String max_enqueue_rate;

    /**
     * Is the dead letter queue configured for the destination.  A
     * dead letter queue is used for storing messages that failed to get processed
     * by consumers.  If not set, then messages that fail to get processed
     * will be dropped.  If '*' appears in the name it will be replaced with
     * the queue's id.
     */
    @XmlAttribute(name="dlq")
    public String dlq;

    /**
     * Once a message has been nacked the configured
     * number of times the message will be considered to be a
     * poison message and will get moved to the dead letter queue if that's
     * configured or dropped.  If set to less than one, then the message
     * will never be considered to be a poison message.
     * Defaults to zero.
     */
    @XmlAttribute(name="nak_limit")
    public Integer nak_limit;

    /**
     * Should expired messages be sent to the dead letter queue?  Defaults to false.
     */
    @XmlAttribute(name="dlq_expired")
    public Boolean dlq_expired;

    /**
     * To hold any other non-matching XML elements
     */
    @XmlAnyElement(lax=true)
    public List<Object> other = new ArrayList<Object>();

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof QueueSettingsDTO)) return false;

        QueueSettingsDTO that = (QueueSettingsDTO) o;

        if (catchup_enqueue_rate != null ? !catchup_enqueue_rate.equals(that.catchup_enqueue_rate) : that.catchup_enqueue_rate != null)
            return false;
        if (dlq != null ? !dlq.equals(that.dlq) : that.dlq != null) return false;
        if (fast_delivery_rate != null ? !fast_delivery_rate.equals(that.fast_delivery_rate) : that.fast_delivery_rate != null)
            return false;
        if (full_policy != null ? !full_policy.equals(that.full_policy) : that.full_policy != null) return false;
        if (max_enqueue_rate != null ? !max_enqueue_rate.equals(that.max_enqueue_rate) : that.max_enqueue_rate != null)
            return false;
        if (nak_limit != null ? !nak_limit.equals(that.nak_limit) : that.nak_limit != null) return false;
        if (other != null ? !other.equals(that.other) : that.other != null) return false;
        if (persistent != null ? !persistent.equals(that.persistent) : that.persistent != null) return false;
        if (quota != null ? !quota.equals(that.quota) : that.quota != null) return false;
        if (swap != null ? !swap.equals(that.swap) : that.swap != null) return false;
        if (swap_range_size != null ? !swap_range_size.equals(that.swap_range_size) : that.swap_range_size != null)
            return false;
        if (tail_buffer != null ? !tail_buffer.equals(that.tail_buffer) : that.tail_buffer != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = tail_buffer != null ? tail_buffer.hashCode() : 0;
        result = 31 * result + (persistent != null ? persistent.hashCode() : 0);
        result = 31 * result + (swap != null ? swap.hashCode() : 0);
        result = 31 * result + (swap_range_size != null ? swap_range_size.hashCode() : 0);
        result = 31 * result + (quota != null ? quota.hashCode() : 0);
        result = 31 * result + (full_policy != null ? full_policy.hashCode() : 0);
        result = 31 * result + (fast_delivery_rate != null ? fast_delivery_rate.hashCode() : 0);
        result = 31 * result + (catchup_enqueue_rate != null ? catchup_enqueue_rate.hashCode() : 0);
        result = 31 * result + (max_enqueue_rate != null ? max_enqueue_rate.hashCode() : 0);
        result = 31 * result + (dlq != null ? dlq.hashCode() : 0);
        result = 31 * result + (nak_limit != null ? nak_limit.hashCode() : 0);
        result = 31 * result + (other != null ? other.hashCode() : 0);
        return result;
    }
}
