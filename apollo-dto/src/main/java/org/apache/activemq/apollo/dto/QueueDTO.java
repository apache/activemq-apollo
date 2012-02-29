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



import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import javax.xml.bind.annotation.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@XmlRootElement(name = "queue")
@XmlAccessorType(XmlAccessType.FIELD)
@JsonIgnoreProperties(ignoreUnknown = true)
public class QueueDTO extends StringIdDTO {

    /**
     * Controls when the queue will auto delete.
     * If set to zero, then the queue will NOT auto
     * delete, otherwise the queue will auto delete
     * after it has been unused for the number
     * of seconds configured in this field.  If unset,
     * it defaults to 5 minutes.
     */
    @XmlAttribute(name="auto_delete_after")
    public Integer auto_delete_after;

    /**
     * If set to true, then once the queue
     * is created all messages sent to the queue
     * will be mirrored to a topic of the same name
     * and all messages sent to the topic will be mirror
     * to the queue.
     */
    @XmlAttribute
    public Boolean mirrored;

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
     * The maximum amount of disk space the queue is allowed
     * to grow to.  If not set then there is no limit.  You can
     * use settings values like: 500mb or 1g just plain 1024000
     */
    @XmlAttribute(name="quota")
    public String quota;

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
     * To hold any other non-matching XML elements
     */
    @XmlAnyElement(lax=true)
    public List<Object> other = new ArrayList<Object>();

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof QueueDTO)) return false;
        if (!super.equals(o)) return false;

        QueueDTO queueDTO = (QueueDTO) o;

        if (auto_delete_after != null ? !auto_delete_after.equals(queueDTO.auto_delete_after) : queueDTO.auto_delete_after != null)
            return false;
        if (fast_delivery_rate != null ? !fast_delivery_rate.equals(queueDTO.fast_delivery_rate) : queueDTO.fast_delivery_rate != null)
            return false;
        if (catchup_enqueue_rate != null ? !catchup_enqueue_rate.equals(queueDTO.catchup_enqueue_rate) : queueDTO.catchup_enqueue_rate != null)
            return false;
        if (max_enqueue_rate != null ? !max_enqueue_rate.equals(queueDTO.max_enqueue_rate) : queueDTO.max_enqueue_rate != null)
            return false;
        if (other != null ? !other.equals(queueDTO.other) : queueDTO.other != null) return false;
        if (persistent != null ? !persistent.equals(queueDTO.persistent) : queueDTO.persistent != null) return false;
        if (quota != null ? !quota.equals(queueDTO.quota) : queueDTO.quota != null) return false;
        if (swap != null ? !swap.equals(queueDTO.swap) : queueDTO.swap != null) return false;
        if (swap_range_size != null ? !swap_range_size.equals(queueDTO.swap_range_size) : queueDTO.swap_range_size != null)
            return false;
        if (mirrored != null ? !mirrored.equals(queueDTO.mirrored) : queueDTO.mirrored != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (auto_delete_after != null ? auto_delete_after.hashCode() : 0);
        result = 31 * result + (mirrored != null ? mirrored.hashCode() : 0);
        result = 31 * result + (persistent != null ? persistent.hashCode() : 0);
        result = 31 * result + (swap != null ? swap.hashCode() : 0);
        result = 31 * result + (swap_range_size != null ? swap_range_size.hashCode() : 0);
        result = 31 * result + (quota != null ? quota.hashCode() : 0);
        result = 31 * result + (fast_delivery_rate != null ? fast_delivery_rate.hashCode() : 0);
        result = 31 * result + (catchup_enqueue_rate != null ? catchup_enqueue_rate.hashCode() : 0);
        result = 31 * result + (max_enqueue_rate != null ? max_enqueue_rate.hashCode() : 0);
        result = 31 * result + (other != null ? other.hashCode() : 0);
        return result;
    }
}
