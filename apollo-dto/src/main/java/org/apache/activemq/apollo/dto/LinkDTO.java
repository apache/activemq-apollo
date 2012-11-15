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
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import javax.xml.bind.annotation.*;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@XmlRootElement(name="link")
@XmlAccessorType(XmlAccessType.FIELD)
@JsonIgnoreProperties(ignoreUnknown = true)
public class LinkDTO {

    @XmlAttribute
    public String kind;

    @XmlAttribute
    public String id;

    @XmlAttribute
    public String label;

    /**
     * The number of messages that have been dispatched over the link
     */
    @XmlAttribute(name="enqueue_item_counter")
    public long enqueue_item_counter;

    /**
     * The total size in bytes of messages that have been dispatched
     * over the link
     */
    @XmlAttribute(name="enqueue_size_counter")
    public long enqueue_size_counter;

    /**
     * Timestamp of when a message last went over the link.
     */
    @XmlAttribute(name="enqueue_tsr")
    public long enqueue_ts;

}