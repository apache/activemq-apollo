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
@XmlType(name = "connector_type")
@XmlAccessorType(XmlAccessType.FIELD)
@JsonTypeInfo(use = JsonTypeInfo.Id.CUSTOM, include = JsonTypeInfo.As.PROPERTY, property = "@class")
@JsonTypeIdResolver(ApolloTypeIdResolver.class)
@JsonIgnoreProperties(ignoreUnknown = true)
abstract public class ConnectorTypeDTO extends ServiceDTO {

    /**
     *  The maximum number of concurrently open connections this connector will accept before it stops accepting
     *  additional connections. If not set, then there is no limit.
     */
    @XmlAttribute(name = "connection_limit")
    public Integer connection_limit;

    /**
     * To hold any other non-matching XML elements
     */
    @XmlAnyElement(lax = true)
    public List<Object> other = new ArrayList<Object>();

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ConnectorTypeDTO)) return false;
        if (!super.equals(o)) return false;

        ConnectorTypeDTO that = (ConnectorTypeDTO) o;

        if (connection_limit != null ? !connection_limit.equals(that.connection_limit) : that.connection_limit != null)
            return false;
        if (other != null ? !other.equals(that.other) : that.other != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (connection_limit != null ? connection_limit.hashCode() : 0);
        result = 31 * result + (other != null ? other.hashCode() : 0);
        return result;
    }
}