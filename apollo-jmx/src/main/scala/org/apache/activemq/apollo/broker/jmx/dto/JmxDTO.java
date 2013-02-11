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
package org.apache.activemq.apollo.broker.jmx.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.activemq.apollo.dto.CustomServiceDTO;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@XmlRootElement(name="jmx")
@XmlAccessorType(XmlAccessType.FIELD)
@JsonIgnoreProperties(ignoreUnknown = true)
public class JmxDTO extends CustomServiceDTO {

    /**
     * Should the broker be listed in JMX.  Defaults to true.
     */
    @XmlAttribute
    public Boolean enabled;


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof JmxDTO)) return false;
        if (!super.equals(o)) return false;

        JmxDTO jmxDTO = (JmxDTO) o;

        if (enabled != null ? !enabled.equals(jmxDTO.enabled) : jmxDTO.enabled != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (enabled != null ? enabled.hashCode() : 0);
        return result;
    }
}