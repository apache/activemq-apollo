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
package org.apache.activemq.apollo.openwire.dto;

import org.apache.activemq.apollo.dto.ProtocolDTO;

import javax.xml.bind.annotation.*;

/**
 * Allow you to customize the openwire protocol implementation.
 */
@XmlRootElement(name="stomp")
@XmlAccessorType(XmlAccessType.FIELD)
public class OpenwireDTO extends ProtocolDTO {

    @XmlAttribute(name="max_data_length")
    public Integer max_data_length;

    @XmlAttribute(name="destination_separator")
    public String destination_separator;

    @XmlAttribute(name="path_separator")
    public String path_separator;

    @XmlAttribute(name="any_child_wildcard")
    public String any_child_wildcard;

    @XmlAttribute(name="any_descendant_wildcard")
    public String any_descendant_wildcard;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof OpenwireDTO)) return false;

        OpenwireDTO openwireDTO = (OpenwireDTO) o;

        if (any_child_wildcard != null ? !any_child_wildcard.equals(openwireDTO.any_child_wildcard) : openwireDTO.any_child_wildcard != null)
            return false;
        if (any_descendant_wildcard != null ? !any_descendant_wildcard.equals(openwireDTO.any_descendant_wildcard) : openwireDTO.any_descendant_wildcard != null)
            return false;
        if (destination_separator != null ? !destination_separator.equals(openwireDTO.destination_separator) : openwireDTO.destination_separator != null)
            return false;
        if (max_data_length != null ? !max_data_length.equals(openwireDTO.max_data_length) : openwireDTO.max_data_length != null)
            return false;
        if (path_separator != null ? !path_separator.equals(openwireDTO.path_separator) : openwireDTO.path_separator != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (max_data_length != null ? max_data_length.hashCode() : 0);
        result = 31 * result + (destination_separator != null ? destination_separator.hashCode() : 0);
        result = 31 * result + (path_separator != null ? path_separator.hashCode() : 0);
        result = 31 * result + (any_child_wildcard != null ? any_child_wildcard.hashCode() : 0);
        result = 31 * result + (any_descendant_wildcard != null ? any_descendant_wildcard.hashCode() : 0);
        return result;
    }
}
