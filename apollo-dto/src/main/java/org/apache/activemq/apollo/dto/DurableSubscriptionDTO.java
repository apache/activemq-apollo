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

import javax.xml.bind.annotation.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@XmlRootElement(name = "dsub")
@XmlAccessorType(XmlAccessType.FIELD)
@JsonIgnoreProperties(ignoreUnknown = true)
public class DurableSubscriptionDTO extends QueueDTO {

    /**
     *  A regular expression used to match the subscription id.
     */
    @XmlAttribute(name="id_regex")
    public String id_regex;

    /**
     * If the topic and id are specified, then the durable subscription
     * can be eagerly created.
     */
    @XmlAttribute
    public String topic;

    /**
     * Additional topic names.
     */
    @XmlElement(name="topic")
    public ArrayList<String> topics = new ArrayList<String>();

    /**
     * An optional selector that the durable subscription will be created
     * with when it's first eagerly created.
     */
    @XmlAttribute
    public String selector;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DurableSubscriptionDTO)) return false;
        if (!super.equals(o)) return false;

        DurableSubscriptionDTO that = (DurableSubscriptionDTO) o;

        if (id_regex != null ? !id_regex.equals(that.id_regex) : that.id_regex != null) return false;
        if (selector != null ? !selector.equals(that.selector) : that.selector != null) return false;
        if (topic != null ? !topic.equals(that.topic) : that.topic != null) return false;
        if (topics != null ? !topics.equals(that.topics) : that.topics != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (id_regex != null ? id_regex.hashCode() : 0);
        result = 31 * result + (topic != null ? topic.hashCode() : 0);
        result = 31 * result + (topics != null ? topics.hashCode() : 0);
        result = 31 * result + (selector != null ? selector.hashCode() : 0);
        return result;
    }
}
