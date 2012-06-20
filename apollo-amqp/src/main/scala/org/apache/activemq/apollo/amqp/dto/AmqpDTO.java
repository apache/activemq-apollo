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
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

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

    @XmlAttribute(name="add_user_header")
    public String add_user_header;

    /**
     * A broker accepts connections via it's configured connectors.
     */
    @XmlElement(name="add_user_header")
    public List<AddUserHeaderDTO> add_user_headers = new ArrayList<AddUserHeaderDTO>();

    /**
     * If set, it will add the configured header name with the value
     * set the a timestamp of when the message is received.
     */
    @XmlAttribute(name="add_timestamp_header")
    public String add_timestamp_header;

    /**
     * If set, the configured header will be added to message
     * sent to consumer if the message is a redelivery.  It will be
     * set to the number of re-deliveries that have occurred.
     */
    @XmlAttribute(name="add_redeliveries_header")
    public String add_redeliveries_header;

    @XmlAttribute(name="max_header_length")
    public String max_header_length;

    @XmlAttribute(name="max_headers")
    public Integer max_headers;

    @XmlAttribute(name="max_data_length")
    public String max_data_length;

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

        if (add_redeliveries_header != null ? !add_redeliveries_header.equals(amqpDTO.add_redeliveries_header) : amqpDTO.add_redeliveries_header != null)
            return false;
        if (add_timestamp_header != null ? !add_timestamp_header.equals(amqpDTO.add_timestamp_header) : amqpDTO.add_timestamp_header != null)
            return false;
        if (add_user_header != null ? !add_user_header.equals(amqpDTO.add_user_header) : amqpDTO.add_user_header != null)
            return false;
        if (add_user_headers != null ? !add_user_headers.equals(amqpDTO.add_user_headers) : amqpDTO.add_user_headers != null)
            return false;
        if (any_child_wildcard != null ? !any_child_wildcard.equals(amqpDTO.any_child_wildcard) : amqpDTO.any_child_wildcard != null)
            return false;
        if (any_descendant_wildcard != null ? !any_descendant_wildcard.equals(amqpDTO.any_descendant_wildcard) : amqpDTO.any_descendant_wildcard != null)
            return false;
        if (buffer_size != null ? !buffer_size.equals(amqpDTO.buffer_size) : amqpDTO.buffer_size != null)
            return false;
        if (destination_separator != null ? !destination_separator.equals(amqpDTO.destination_separator) : amqpDTO.destination_separator != null)
            return false;
        if (die_delay != null ? !die_delay.equals(amqpDTO.die_delay) : amqpDTO.die_delay != null) return false;
        if (max_data_length != null ? !max_data_length.equals(amqpDTO.max_data_length) : amqpDTO.max_data_length != null)
            return false;
        if (max_header_length != null ? !max_header_length.equals(amqpDTO.max_header_length) : amqpDTO.max_header_length != null)
            return false;
        if (max_headers != null ? !max_headers.equals(amqpDTO.max_headers) : amqpDTO.max_headers != null)
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

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (add_user_header != null ? add_user_header.hashCode() : 0);
        result = 31 * result + (add_user_headers != null ? add_user_headers.hashCode() : 0);
        result = 31 * result + (add_timestamp_header != null ? add_timestamp_header.hashCode() : 0);
        result = 31 * result + (add_redeliveries_header != null ? add_redeliveries_header.hashCode() : 0);
        result = 31 * result + (max_header_length != null ? max_header_length.hashCode() : 0);
        result = 31 * result + (max_headers != null ? max_headers.hashCode() : 0);
        result = 31 * result + (max_data_length != null ? max_data_length.hashCode() : 0);
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
