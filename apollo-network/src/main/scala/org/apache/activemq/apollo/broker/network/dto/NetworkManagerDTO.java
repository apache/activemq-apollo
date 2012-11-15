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

import org.apache.activemq.apollo.dto.CustomServiceDTO;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import javax.xml.bind.annotation.*;
import java.util.ArrayList;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@XmlRootElement(name="network_manager")
@XmlAccessorType(XmlAccessType.FIELD)
@JsonIgnoreProperties(ignoreUnknown = true)
public class NetworkManagerDTO extends CustomServiceDTO {

    @XmlAttribute(name="user")
    public String user;

    @XmlAttribute(name="password")
    public String password;

    @XmlAttribute(name="duplex")
    public Boolean duplex;

    @XmlAttribute(name="monitoring_interval")
    public Integer monitoring_interval;

    @XmlElement(name="self")
    public ClusterMemberDTO self = null;

    @XmlElement(name="member")
    public ArrayList<ClusterMemberDTO> members = new ArrayList<ClusterMemberDTO>();

    @XmlElementRef()
    public ArrayList<MembershipMonitorDTO> membership_monitors = new ArrayList<MembershipMonitorDTO>();

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NetworkManagerDTO)) return false;
        if (!super.equals(o)) return false;

        NetworkManagerDTO that = (NetworkManagerDTO) o;

        if (duplex != null ? !duplex.equals(that.duplex) : that.duplex != null)
            return false;
        if (members != null ? !members.equals(that.members) : that.members != null)
            return false;
        if (monitoring_interval != null ? !monitoring_interval.equals(that.monitoring_interval) : that.monitoring_interval != null)
            return false;
        if (password != null ? !password.equals(that.password) : that.password != null)
            return false;
        if (self != null ? !self.equals(that.self) : that.self != null)
            return false;
        if (user != null ? !user.equals(that.user) : that.user != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (user != null ? user.hashCode() : 0);
        result = 31 * result + (password != null ? password.hashCode() : 0);
        result = 31 * result + (self != null ? self.hashCode() : 0);
        result = 31 * result + (duplex != null ? duplex.hashCode() : 0);
        result = 31 * result + (monitoring_interval != null ? monitoring_interval.hashCode() : 0);
        result = 31 * result + (members != null ? members.hashCode() : 0);
        return result;
    }
}
