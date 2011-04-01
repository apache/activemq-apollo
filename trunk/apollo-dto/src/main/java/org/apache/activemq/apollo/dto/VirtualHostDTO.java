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

import java.util.ArrayList;

import javax.xml.bind.annotation.*;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@XmlRootElement(name = "virtual_host")
@XmlAccessorType(XmlAccessType.FIELD)
public class VirtualHostDTO extends ServiceDTO {

    @XmlElement(name="host_name", required=true)
    public ArrayList<String> host_names = new ArrayList<String>();

    @XmlElementRef
    public StoreDTO store;

    /**
     * Should destinations be auto created when they are first accessed
     * by clients?
     */
    @XmlAttribute(name="auto_create_destinations")
    public Boolean auto_create_destinations;

    /**
     * Should queues be purged on startup?
     */
    @XmlAttribute(name="purge_on_startup")
    public Boolean purge_on_startup;

    /**
     * Holds the configuration for the destinations.
     */
    @XmlElement(name="topic")
    public ArrayList<TopicDTO> topics = new ArrayList<TopicDTO>();

    /**
     * Holds the configuration for the queues.
     */
    @XmlElement(name="queue")
    public ArrayList<QueueDTO> queues = new ArrayList<QueueDTO>();

    /**
     * Holds the configuration for the queues.
     */
    @XmlElement(name="durable_subscription")
    public ArrayList<DurableSubscriptionDTO> durable_subscriptions = new ArrayList<DurableSubscriptionDTO>();

    /**
     * Should connections get regroups so they get serviced by the same thread?
     */
    @XmlAttribute(name="regroup_connections")
    public Boolean regroup_connections;

    @XmlElement(name="acl")
    public VirtualHostAclDTO acl;

    @XmlElement(name="authentication")
    public AuthenticationDTO authentication;

    @XmlElementRef
    public RouterDTO router;

    @XmlElement(name="log_category")
    public LogCategoryDTO log_category;
}
