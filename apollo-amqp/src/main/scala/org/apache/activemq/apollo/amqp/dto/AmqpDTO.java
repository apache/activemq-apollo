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
package org.apache.activemq.apollo.amqp.dto;

import org.apache.activemq.apollo.dto.AddUserHeaderDTO;
import org.apache.activemq.apollo.dto.ProtocolDTO;
import org.apache.activemq.apollo.dto.ProtocolFilterDTO;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import javax.xml.bind.annotation.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Allow you to customize the amqp protocol implementation.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@XmlRootElement(name="amqp")
@XmlAccessorType(XmlAccessType.FIELD)
@JsonIgnoreProperties(ignoreUnknown = true)
public class AmqpDTO extends ProtocolDTO {

    @XmlAttribute(name="trace")
    public Boolean trace;

    @XmlAttribute(name="max_frame_size")
    public String max_frame_size;

    @XmlElementRef
    public List<ProtocolFilterDTO> protocol_filters = new ArrayList<ProtocolFilterDTO>();

    @XmlAttribute(name="queue_prefix")
    public String queue_prefix;

    @XmlAttribute(name="topic_prefix")
    public String topic_prefix;

    @XmlAttribute(name="temp_queue_prefix")
    public String temp_queue_prefix;

    @XmlAttribute(name="temp_topic_prefix")
    public String temp_topic_prefix;

    @XmlAttribute(name="destination_separator")
    public String destination_separator;

    @XmlAttribute(name="path_separator")
    public String path_separator;

    @XmlAttribute(name="any_child_wildcard")
    public String any_child_wildcard;

    @XmlAttribute(name="any_descendant_wildcard")
    public String any_descendant_wildcard;

    @XmlAttribute(name="regex_wildcard_start")
    public String regex_wildcard_start;

    @XmlAttribute(name="regex_wildcard_end")
    public String regex_wildcard_end;

    @XmlAttribute(name="die_delay")
    public Long die_delay;

    @XmlAttribute(name="buffer_size")
    public String buffer_size;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AmqpDTO)) return false;
        if (!super.equals(o)) return false;

        AmqpDTO amqpDTO = (AmqpDTO) o;

        if (any_child_wildcard != null ? !any_child_wildcard.equals(amqpDTO.any_child_wildcard) : amqpDTO.any_child_wildcard != null)
            return false;
        if (any_descendant_wildcard != null ? !any_descendant_wildcard.equals(amqpDTO.any_descendant_wildcard) : amqpDTO.any_descendant_wildcard != null)
            return false;
        if (buffer_size != null ? !buffer_size.equals(amqpDTO.buffer_size) : amqpDTO.buffer_size != null)
            return false;
        if (destination_separator != null ? !destination_separator.equals(amqpDTO.destination_separator) : amqpDTO.destination_separator != null)
            return false;
        if (die_delay != null ? !die_delay.equals(amqpDTO.die_delay) : amqpDTO.die_delay != null)
            return false;
        if (max_frame_size != null ? !max_frame_size.equals(amqpDTO.max_frame_size) : amqpDTO.max_frame_size != null)
            return false;
        if (path_separator != null ? !path_separator.equals(amqpDTO.path_separator) : amqpDTO.path_separator != null)
            return false;
        if (protocol_filters != null ? !protocol_filters.equals(amqpDTO.protocol_filters) : amqpDTO.protocol_filters != null)
            return false;
        if (queue_prefix != null ? !queue_prefix.equals(amqpDTO.queue_prefix) : amqpDTO.queue_prefix != null)
            return false;
        if (regex_wildcard_end != null ? !regex_wildcard_end.equals(amqpDTO.regex_wildcard_end) : amqpDTO.regex_wildcard_end != null)
            return false;
        if (regex_wildcard_start != null ? !regex_wildcard_start.equals(amqpDTO.regex_wildcard_start) : amqpDTO.regex_wildcard_start != null)
            return false;
        if (temp_queue_prefix != null ? !temp_queue_prefix.equals(amqpDTO.temp_queue_prefix) : amqpDTO.temp_queue_prefix != null)
            return false;
        if (temp_topic_prefix != null ? !temp_topic_prefix.equals(amqpDTO.temp_topic_prefix) : amqpDTO.temp_topic_prefix != null)
            return false;
        if (topic_prefix != null ? !topic_prefix.equals(amqpDTO.topic_prefix) : amqpDTO.topic_prefix != null)
            return false;
        if (trace != null ? !trace.equals(amqpDTO.trace) : amqpDTO.trace != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (trace != null ? trace.hashCode() : 0);
        result = 31 * result + (max_frame_size != null ? max_frame_size.hashCode() : 0);
        result = 31 * result + (protocol_filters != null ? protocol_filters.hashCode() : 0);
        result = 31 * result + (queue_prefix != null ? queue_prefix.hashCode() : 0);
        result = 31 * result + (topic_prefix != null ? topic_prefix.hashCode() : 0);
        result = 31 * result + (temp_queue_prefix != null ? temp_queue_prefix.hashCode() : 0);
        result = 31 * result + (temp_topic_prefix != null ? temp_topic_prefix.hashCode() : 0);
        result = 31 * result + (destination_separator != null ? destination_separator.hashCode() : 0);
        result = 31 * result + (path_separator != null ? path_separator.hashCode() : 0);
        result = 31 * result + (any_child_wildcard != null ? any_child_wildcard.hashCode() : 0);
        result = 31 * result + (any_descendant_wildcard != null ? any_descendant_wildcard.hashCode() : 0);
        result = 31 * result + (regex_wildcard_start != null ? regex_wildcard_start.hashCode() : 0);
        result = 31 * result + (regex_wildcard_end != null ? regex_wildcard_end.hashCode() : 0);
        result = 31 * result + (die_delay != null ? die_delay.hashCode() : 0);
        result = 31 * result + (buffer_size != null ? buffer_size.hashCode() : 0);
        return result;
    }
}
