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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import javax.xml.bind.annotation.*;

/**
 * AllowS you to customize the openwire protocol implementation.
 */
@XmlRootElement(name="openwire")
@XmlAccessorType(XmlAccessType.FIELD)
@JsonIgnoreProperties(ignoreUnknown = true)
public class OpenwireDTO extends ProtocolDTO {

    public Integer version;
    @XmlAttribute(name="stack_trace")
    public Boolean stack_trace;
    @XmlAttribute(name="cache")
    public Boolean cache;
    @XmlAttribute(name="cache_size")
    public Integer cache_size;
    @XmlAttribute(name="tight_encoding")
    public Boolean tight_encoding;
    @XmlAttribute(name="tcp_no_delay")
    public Boolean tcp_no_delay;
    @XmlAttribute(name="max_inactivity_duration")
    public Long max_inactivity_duration;
    @XmlAttribute(name="max_inactivity_duration_initial_delay")
    public Long max_inactivity_duration_initial_delay;
    @XmlAttribute(name="max_frame_size")
    public Long max_frame_size;
    @XmlAttribute(name="buffer_size")
    public String buffer_size;
    @XmlAttribute(name="add_jmsxuserid")
    public Boolean add_jmsxuserid;


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof OpenwireDTO)) return false;
        if (!super.equals(o)) return false;

        OpenwireDTO that = (OpenwireDTO) o;

        if (buffer_size != null ? !buffer_size.equals(that.buffer_size) : that.buffer_size != null)
            return false;
        if (cache != null ? !cache.equals(that.cache) : that.cache != null)
            return false;
        if (cache_size != null ? !cache_size.equals(that.cache_size) : that.cache_size != null)
            return false;
        if (add_jmsxuserid != null ? !add_jmsxuserid.equals(that.add_jmsxuserid) : that.add_jmsxuserid != null)
            return false;
        if (max_frame_size != null ? !max_frame_size.equals(that.max_frame_size) : that.max_frame_size != null)
            return false;
        if (max_inactivity_duration != null ? !max_inactivity_duration.equals(that.max_inactivity_duration) : that.max_inactivity_duration != null)
            return false;
        if (max_inactivity_duration_initial_delay != null ? !max_inactivity_duration_initial_delay.equals(that.max_inactivity_duration_initial_delay) : that.max_inactivity_duration_initial_delay != null)
            return false;
        if (stack_trace != null ? !stack_trace.equals(that.stack_trace) : that.stack_trace != null)
            return false;
        if (tcp_no_delay != null ? !tcp_no_delay.equals(that.tcp_no_delay) : that.tcp_no_delay != null)
            return false;
        if (tight_encoding != null ? !tight_encoding.equals(that.tight_encoding) : that.tight_encoding != null)
            return false;
        if (version != null ? !version.equals(that.version) : that.version != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (version != null ? version.hashCode() : 0);
        result = 31 * result + (stack_trace != null ? stack_trace.hashCode() : 0);
        result = 31 * result + (cache != null ? cache.hashCode() : 0);
        result = 31 * result + (cache_size != null ? cache_size.hashCode() : 0);
        result = 31 * result + (tight_encoding != null ? tight_encoding.hashCode() : 0);
        result = 31 * result + (tcp_no_delay != null ? tcp_no_delay.hashCode() : 0);
        result = 31 * result + (max_inactivity_duration != null ? max_inactivity_duration.hashCode() : 0);
        result = 31 * result + (max_inactivity_duration_initial_delay != null ? max_inactivity_duration_initial_delay.hashCode() : 0);
        result = 31 * result + (max_frame_size != null ? max_frame_size.hashCode() : 0);
        result = 31 * result + (buffer_size != null ? buffer_size.hashCode() : 0);
        result = 31 * result + (add_jmsxuserid != null ? add_jmsxuserid.hashCode() : 0);
        return result;
    }
}
