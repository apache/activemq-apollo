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
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.*;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@JsonTypeInfo(use=JsonTypeInfo.Id.CUSTOM, include=JsonTypeInfo.As.PROPERTY, property="@class")
@JsonTypeIdResolver(ApolloTypeIdResolver.class)
@XmlRootElement(name = "virtual_host")
@XmlAccessorType(XmlAccessType.FIELD)
@JsonIgnoreProperties(ignoreUnknown = true)
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

    @XmlElement(name="access_rule")
    public List<AccessRuleDTO> access_rules = new ArrayList<AccessRuleDTO>();

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
    @XmlElement(name="dsub")
    public ArrayList<DurableSubscriptionDTO> dsubs = new ArrayList<DurableSubscriptionDTO>();

    /**
     * Should connections get regroups so they get serviced by the same thread?
     */
    @XmlAttribute(name="regroup_connections")
    public Boolean regroup_connections;

    @XmlElement(name="authentication")
    public AuthenticationDTO authentication;

    @XmlElement(name="log_category")
    public LogCategoryDTO log_category;

    /**
     * If set the the broker will avoid allocating messages larger than the configured
     * setting on the JVM heap.  They will be held in temp files until consumed or persisted
     */
    @XmlElement(name="heap_bypass")
    public String heap_bypass;

    /**
     * To hold any other non-matching XML elements
     */
    @XmlAnyElement(lax=true)
    public List<Object> other = new ArrayList<Object>();

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof VirtualHostDTO)) return false;
        if (!super.equals(o)) return false;

        VirtualHostDTO that = (VirtualHostDTO) o;

        if (access_rules != null ? !access_rules.equals(that.access_rules) : that.access_rules != null) return false;
        if (authentication != null ? !authentication.equals(that.authentication) : that.authentication != null)
            return false;
        if (auto_create_destinations != null ? !auto_create_destinations.equals(that.auto_create_destinations) : that.auto_create_destinations != null)
            return false;
        if (dsubs != null ? !dsubs.equals(that.dsubs) : that.dsubs != null) return false;
        if (heap_bypass != null ? !heap_bypass.equals(that.heap_bypass) : that.heap_bypass != null) return false;
        if (host_names != null ? !host_names.equals(that.host_names) : that.host_names != null) return false;
        if (log_category != null ? !log_category.equals(that.log_category) : that.log_category != null) return false;
        if (other != null ? !other.equals(that.other) : that.other != null) return false;
        if (purge_on_startup != null ? !purge_on_startup.equals(that.purge_on_startup) : that.purge_on_startup != null)
            return false;
        if (queues != null ? !queues.equals(that.queues) : that.queues != null) return false;
        if (regroup_connections != null ? !regroup_connections.equals(that.regroup_connections) : that.regroup_connections != null)
            return false;
        if (store != null ? !store.equals(that.store) : that.store != null) return false;
        if (topics != null ? !topics.equals(that.topics) : that.topics != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (host_names != null ? host_names.hashCode() : 0);
        result = 31 * result + (store != null ? store.hashCode() : 0);
        result = 31 * result + (auto_create_destinations != null ? auto_create_destinations.hashCode() : 0);
        result = 31 * result + (purge_on_startup != null ? purge_on_startup.hashCode() : 0);
        result = 31 * result + (access_rules != null ? access_rules.hashCode() : 0);
        result = 31 * result + (topics != null ? topics.hashCode() : 0);
        result = 31 * result + (queues != null ? queues.hashCode() : 0);
        result = 31 * result + (dsubs != null ? dsubs.hashCode() : 0);
        result = 31 * result + (regroup_connections != null ? regroup_connections.hashCode() : 0);
        result = 31 * result + (authentication != null ? authentication.hashCode() : 0);
        result = 31 * result + (log_category != null ? log_category.hashCode() : 0);
        result = 31 * result + (heap_bypass != null ? heap_bypass.hashCode() : 0);
        result = 31 * result + (other != null ? other.hashCode() : 0);
        return result;
    }
}
