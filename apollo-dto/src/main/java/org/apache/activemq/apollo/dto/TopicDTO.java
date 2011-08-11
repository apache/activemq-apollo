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
@XmlRootElement(name = "topic")
@XmlAccessorType(XmlAccessType.FIELD)
public class TopicDTO extends StringIdDTO {

    /**
     * Controls when the topic will auto delete.
     * If set to zero, then the topic will NOT auto
     * delete, otherwise the topic will auto delete
     * after it has been unused for the number
     * of seconds configured in this field.  If unset,
     * it defaults to 5 minutes
     */
    @XmlAttribute(name="auto_delete_after")
    public Integer auto_delete_after;

    @XmlAttribute(name="slow_consumer_policy")
    public String slow_consumer_policy;

    @XmlElement(name="acl")
    public TopicAclDTO acl;

    /**
     * To hold any other non-matching XML elements
     */
    @XmlAnyElement(lax=true)
    public List<Object> other = new ArrayList<Object>();

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TopicDTO)) return false;
        if (!super.equals(o)) return false;

        TopicDTO topicDTO = (TopicDTO) o;

        if (acl != null ? !acl.equals(topicDTO.acl) : topicDTO.acl != null) return false;
        if (auto_delete_after != null ? !auto_delete_after.equals(topicDTO.auto_delete_after) : topicDTO.auto_delete_after != null)
            return false;
        if (other != null ? !other.equals(topicDTO.other) : topicDTO.other != null) return false;
        if (slow_consumer_policy != null ? !slow_consumer_policy.equals(topicDTO.slow_consumer_policy) : topicDTO.slow_consumer_policy != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (auto_delete_after != null ? auto_delete_after.hashCode() : 0);
        result = 31 * result + (slow_consumer_policy != null ? slow_consumer_policy.hashCode() : 0);
        result = 31 * result + (acl != null ? acl.hashCode() : 0);
        result = 31 * result + (other != null ? other.hashCode() : 0);
        return result;
    }
}
