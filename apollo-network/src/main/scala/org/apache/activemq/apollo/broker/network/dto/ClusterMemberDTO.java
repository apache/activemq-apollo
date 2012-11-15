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
package org.apache.activemq.apollo.broker.network.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import javax.xml.bind.annotation.*;
import java.util.ArrayList;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@XmlAccessorType(XmlAccessType.FIELD)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ClusterMemberDTO {

    @XmlAttribute(name="id")
    public String id;

    @XmlAttribute(name="cluster")
    public String cluster;

    @XmlElement(name="service")
    public ArrayList<ClusterServiceDTO> services = new ArrayList<ClusterServiceDTO>();

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ClusterMemberDTO)) return false;

        ClusterMemberDTO that = (ClusterMemberDTO) o;

        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        if (cluster != null ? !cluster.equals(that.id) : that.cluster != null) return false;
        if (services != null ? !services.equals(that.services) : that.services != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (cluster != null ? cluster.hashCode() : 0);
        result = 31 * result + (services != null ? services.hashCode() : 0);
        return result;
    }
}
