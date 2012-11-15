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
import java.util.List;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@JsonTypeInfo(use=JsonTypeInfo.Id.CUSTOM, include=JsonTypeInfo.As.PROPERTY, property="@class")
@JsonTypeIdResolver(ApolloTypeIdResolver.class)
@XmlType (name = "protocol_type")
@XmlRootElement(name="protocol")
@XmlAccessorType(XmlAccessType.FIELD)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ProtocolDTO {

    /**
     * To hold any other non-matching XML elements
     */
    @XmlAnyElement(lax=true)
    public List<Object> other = new ArrayList<Object>();

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ProtocolDTO)) return false;

        ProtocolDTO that = (ProtocolDTO) o;

        if (other != null ? !other.equals(that.other) : that.other != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return other != null ? other.hashCode() : 0;
    }
}
