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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import javax.xml.bind.annotation.*;
import java.util.ArrayList;

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@XmlRootElement(name = "dsub_destination")
@XmlAccessorType(XmlAccessType.FIELD)
@JsonIgnoreProperties(ignoreUnknown = true)
public class DurableSubscriptionDestinationDTO extends DestinationDTO {

    @XmlAttribute
    public String selector;

    /**
     * Topics that the durable subscription is attached to
     */
    @XmlElement(name="topic")
    public ArrayList<TopicDestinationDTO> topics = new ArrayList<TopicDestinationDTO>();

    public DurableSubscriptionDestinationDTO() {
    }

    public DurableSubscriptionDestinationDTO(String subscription_id) {
        super(subscription_id);
    }

    @JsonIgnore
    public String subscription_id() {
        return name;
    }

    /**
     * Marks the destination as addressing a durable subscription directly.
     * This will not create or modify an existing subscription.
     */
    @JsonIgnore
    public DurableSubscriptionDestinationDTO direct() {
        topics = null;
        return this;
    }

    @JsonIgnore
    public boolean is_direct() {
        return topics == null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DurableSubscriptionDestinationDTO)) return false;
        if (!super.equals(o)) return false;

        DurableSubscriptionDestinationDTO that = (DurableSubscriptionDestinationDTO) o;

        if (selector != null ? !selector.equals(that.selector) : that.selector != null) return false;
        if (topics != null ? !topics.equals(that.topics) : that.topics != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (selector != null ? selector.hashCode() : 0);
        result = 31 * result + (topics != null ? topics.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "dsub:"+name;
    }
}