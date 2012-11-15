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
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver;

import javax.xml.bind.annotation.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@XmlType(name = "destination")
@XmlSeeAlso({QueueDestinationDTO.class, DurableSubscriptionDestinationDTO.class})
@JsonTypeInfo(use = JsonTypeInfo.Id.CUSTOM, include = JsonTypeInfo.As.PROPERTY, property = "@class")
@JsonTypeIdResolver(ApolloTypeIdResolver.class)
@JsonIgnoreProperties(ignoreUnknown = true)
@XmlAccessorType(XmlAccessType.FIELD)
abstract public class DestinationDTO {

    @XmlAttribute(name = "name")
    public String name;

    public DestinationDTO() {
    }
    public DestinationDTO(String name) {
        this.name = name;
    }

    /**
     * @deprecated 
     */
    @XmlElement(name = "path")
    public List<String> path = new ArrayList<String>();

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DestinationDTO)) return false;

        DestinationDTO that = (DestinationDTO) o;

        if (!name.equals(that.name)) return false;
        if (path != null ? !path.equals(that.path) : that.path != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (path != null ? path.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "DestinationDTO{" +
                "name=" + name +
                '}';
    }

}