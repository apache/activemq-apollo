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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Configuration for the udp protocol.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@XmlRootElement(name="udp")
@XmlAccessorType(XmlAccessType.FIELD)
@JsonIgnoreProperties(ignoreUnknown = true)
public class UdpDTO extends ProtocolDTO {

    @XmlAttribute(name="buffer_size")
    public String buffer_size;

    @XmlAttribute(name="topic")
    public String topic;

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof UdpDTO)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        UdpDTO udpDTO = (UdpDTO) o;

        if (buffer_size != null ? !buffer_size.equals(udpDTO.buffer_size) : udpDTO.buffer_size != null) {
            return false;
        }
        if (topic != null ? !topic.equals(udpDTO.topic) : udpDTO.topic != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (buffer_size != null ? buffer_size.hashCode() : 0);
        result = 31 * result + (topic != null ? topic.hashCode() : 0);
        return result;
    }
}
