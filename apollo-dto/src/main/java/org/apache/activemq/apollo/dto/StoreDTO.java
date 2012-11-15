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

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlSeeAlso;
import javax.xml.bind.annotation.XmlType;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@XmlType (name = "store_type")
@JsonTypeIdResolver(ApolloTypeIdResolver.class)
@JsonTypeInfo(use=JsonTypeInfo.Id.CUSTOM, include=JsonTypeInfo.As.PROPERTY, property="@class")
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class StoreDTO {

    /**
     * The flush delay is the amount of time in milliseconds that a store
     * will delay persisting a messaging unit of work in hopes that it will
     * be invalidated shortly thereafter by another unit of work which
     * would negate the operation.
     */
    @XmlAttribute(name="flush_delay", required=false)
    public Long flush_delay;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StoreDTO)) return false;

        StoreDTO storeDTO = (StoreDTO) o;

        if (flush_delay != null ? !flush_delay.equals(storeDTO.flush_delay) : storeDTO.flush_delay != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        return flush_delay != null ? flush_delay.hashCode() : 0;
    }
}
