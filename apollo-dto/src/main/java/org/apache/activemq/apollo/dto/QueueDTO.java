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

}
