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
public class QueueDTO {

    /*
     * The destination name this queue is associated with.  You can use wild cards.
     */
    @XmlAttribute
    public String name;

    /*
     * The kind of queue.  I
     * If not set, then this configuration applies to all queue types.
     */
    @XmlAttribute
    public String kind;

    /**
     * If the kind is "ds" then you can specify which client
     * id this configuration should match.
     */
    @XmlAttribute(name="client_id")
    public String client_id;

    /**
     * If the kind is "ds" then you can specify which subscription
     * id this configuration should match.
     */
    @XmlAttribute(name="subscription_id")
    public String subscription_id;


    /**
     *  The amount of memory buffer space for receiving messages.
     */
    @XmlAttribute(name="producer_buffer")
    public Integer producer_buffer;

    /**
     *  The amount of memory buffer space for the queue..
     */
    @XmlAttribute(name="queue_buffer")
    public Integer queue_buffer;

    /**
     *  The amount of memory buffer space to use per subscription.
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
     * from the store at a time.  Not that swapped entries are just
     * reference pointers to the actual messages.  When not loaded,
     * the batch is referenced as sequence range to conserve memory.
     */
    @XmlAttribute(name="swap_range_size")
    public Integer swap_range_size;

    @XmlElement(name="acl")
    public QueueAclDTO acl;

}
