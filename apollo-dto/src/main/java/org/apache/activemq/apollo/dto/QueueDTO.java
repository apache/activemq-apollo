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



import javax.xml.bind.annotation.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@XmlRootElement(name = "queue")
@XmlAccessorType(XmlAccessType.FIELD)
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
     * If set to true, then routing then there is no difference between
     * sending to a queue or topic of the same name.  The first time
     * a queue is created, it will act like if a durable
     * subscription was created on the topic.
     */
    @XmlAttribute
    public Boolean unified;

    /**
     *  The amount of memory buffer space for the queue..
     */
    @XmlAttribute(name="queue_buffer")
    public Integer queue_buffer;

    /**
     *  The amount of memory buffer space to use per consumer.
     */
    @XmlAttribute(name="consumer_buffer")
    public Integer consumer_buffer;

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

    @XmlElement(name="acl")
    public QueueAclDTO acl;

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

        if (acl != null ? !acl.equals(queueDTO.acl) : queueDTO.acl != null) return false;
        if (auto_delete_after != null ? !auto_delete_after.equals(queueDTO.auto_delete_after) : queueDTO.auto_delete_after != null)
            return false;
        if (consumer_buffer != null ? !consumer_buffer.equals(queueDTO.consumer_buffer) : queueDTO.consumer_buffer != null)
            return false;
        if (other != null ? !other.equals(queueDTO.other) : queueDTO.other != null) return false;
        if (persistent != null ? !persistent.equals(queueDTO.persistent) : queueDTO.persistent != null) return false;
        if (queue_buffer != null ? !queue_buffer.equals(queueDTO.queue_buffer) : queueDTO.queue_buffer != null)
            return false;
        if (swap != null ? !swap.equals(queueDTO.swap) : queueDTO.swap != null) return false;
        if (swap_range_size != null ? !swap_range_size.equals(queueDTO.swap_range_size) : queueDTO.swap_range_size != null)
            return false;
        if (unified != null ? !unified.equals(queueDTO.unified) : queueDTO.unified != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (auto_delete_after != null ? auto_delete_after.hashCode() : 0);
        result = 31 * result + (unified != null ? unified.hashCode() : 0);
        result = 31 * result + (queue_buffer != null ? queue_buffer.hashCode() : 0);
        result = 31 * result + (consumer_buffer != null ? consumer_buffer.hashCode() : 0);
        result = 31 * result + (persistent != null ? persistent.hashCode() : 0);
        result = 31 * result + (swap != null ? swap.hashCode() : 0);
        result = 31 * result + (swap_range_size != null ? swap_range_size.hashCode() : 0);
        result = 31 * result + (acl != null ? acl.hashCode() : 0);
        result = 31 * result + (other != null ? other.hashCode() : 0);
        return result;
    }
}
