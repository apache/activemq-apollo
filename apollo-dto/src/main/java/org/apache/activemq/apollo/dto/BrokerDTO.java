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
 * This is the root container for a broker's configuration.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@XmlRootElement(name="broker")
@XmlAccessorType(XmlAccessType.FIELD)
public class BrokerDTO {

    /**
     * Used to store any configuration notes.
     */
    @XmlElement
    public String notes;

    /**
     * A broker can service many virtual hosts.
     */
    @XmlElementRef
    public List<VirtualHostDTO> virtual_hosts = new ArrayList<VirtualHostDTO>();

    /**
     * A broker accepts connections via it's configured connectors.
     */
    @XmlElementRef
    public List<ConnectorTypeDTO> connectors = new ArrayList<ConnectorTypeDTO>();

    /**
     * The clients should use to connect to this
     * broker.
     */
    @XmlElement(name="client_address")
    public String client_address;

    /**
     * The base data directory of the broker.  It will store
     * persistent data under it.
     */
    @XmlElementRef
    public KeyStorageDTO key_storage;

    @XmlElement(name="acl")
    public BrokerAclDTO acl;

    @XmlElement(name="web_admin")
    public List<WebAdminDTO> web_admins = new ArrayList<WebAdminDTO>();

    @XmlElement(name="authentication")
    public AuthenticationDTO authentication;

    @XmlElement(name="log_category")
    public LogCategoryDTO log_category;

    /**
     * Opaque service class names which gets started/stopped when the broker
     * starts/stops.
     */
    @XmlElement(name="service")
    public List<CustomServiceDTO> services = new ArrayList<CustomServiceDTO>();

    @Deprecated
    @XmlAttribute(name="sticky_dispatching")
    public Boolean sticky_dispatching;

    /**
     * If set to strict, then the broker will not start up if there
     * are any validation errors in the configuration file.
     */
    @XmlAttribute(name="validation")
    public String validation;

    /**
     * To hold any other non-matching XML elements
     */
    @XmlAnyElement(lax=true)
    public List<Object> other = new ArrayList<Object>();

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BrokerDTO)) return false;

        BrokerDTO brokerDTO = (BrokerDTO) o;

        if (acl != null ? !acl.equals(brokerDTO.acl) : brokerDTO.acl != null) return false;
        if (authentication != null ? !authentication.equals(brokerDTO.authentication) : brokerDTO.authentication != null)
            return false;
        if (client_address != null ? !client_address.equals(brokerDTO.client_address) : brokerDTO.client_address != null)
            return false;
        if (connectors != null ? !connectors.equals(brokerDTO.connectors) : brokerDTO.connectors != null) return false;
        if (key_storage != null ? !key_storage.equals(brokerDTO.key_storage) : brokerDTO.key_storage != null)
            return false;
        if (log_category != null ? !log_category.equals(brokerDTO.log_category) : brokerDTO.log_category != null)
            return false;
        if (notes != null ? !notes.equals(brokerDTO.notes) : brokerDTO.notes != null) return false;
        if (other != null ? !other.equals(brokerDTO.other) : brokerDTO.other != null) return false;
        if (services != null ? !services.equals(brokerDTO.services) : brokerDTO.services != null) return false;
        if (sticky_dispatching != null ? !sticky_dispatching.equals(brokerDTO.sticky_dispatching) : brokerDTO.sticky_dispatching != null)
            return false;
        if (validation != null ? !validation.equals(brokerDTO.validation) : brokerDTO.validation != null) return false;
        if (virtual_hosts != null ? !virtual_hosts.equals(brokerDTO.virtual_hosts) : brokerDTO.virtual_hosts != null)
            return false;
        if (web_admins != null ? !web_admins.equals(brokerDTO.web_admins) : brokerDTO.web_admins != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = notes != null ? notes.hashCode() : 0;
        result = 31 * result + (virtual_hosts != null ? virtual_hosts.hashCode() : 0);
        result = 31 * result + (connectors != null ? connectors.hashCode() : 0);
        result = 31 * result + (client_address != null ? client_address.hashCode() : 0);
        result = 31 * result + (key_storage != null ? key_storage.hashCode() : 0);
        result = 31 * result + (acl != null ? acl.hashCode() : 0);
        result = 31 * result + (web_admins != null ? web_admins.hashCode() : 0);
        result = 31 * result + (authentication != null ? authentication.hashCode() : 0);
        result = 31 * result + (log_category != null ? log_category.hashCode() : 0);
        result = 31 * result + (services != null ? services.hashCode() : 0);
        result = 31 * result + (sticky_dispatching != null ? sticky_dispatching.hashCode() : 0);
        result = 31 * result + (validation != null ? validation.hashCode() : 0);
        result = 31 * result + (other != null ? other.hashCode() : 0);
        return result;
    }
}
