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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@XmlRootElement(name = "dsub_destination")
@XmlAccessorType(XmlAccessType.FIELD)
public class DurableSubscriptionDestinationDTO extends TopicDestinationDTO {

    @XmlAttribute
    public String selector;

    @XmlAttribute(name="subscription_id")
    public String subscription_id;

    public DurableSubscriptionDestinationDTO() {
    }

    public DurableSubscriptionDestinationDTO(String subscription_id) {
        super();
        this.subscription_id = subscription_id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        DurableSubscriptionDestinationDTO that = (DurableSubscriptionDestinationDTO) o;

        if (selector != null ? !selector.equals(that.selector) : that.selector != null) return false;
        if (subscription_id != null ? !subscription_id.equals(that.subscription_id) : that.subscription_id != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (selector != null ? selector.hashCode() : 0);
        result = 31 * result + (subscription_id != null ? subscription_id.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "DurableSubscriptionDestinationDTO{" +
                "path=" + path +
                ", selector='" + selector + '\'' +
                ", subscription_id='" + subscription_id + '\'' +
                '}';
    }
}