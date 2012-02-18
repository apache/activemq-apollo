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
 * AllowS you to customize the openwire protocol implementation.
 */
@XmlRootElement(name="openwire")
@XmlAccessorType(XmlAccessType.FIELD)
public class OpenwireDTO extends ProtocolDTO {

    @XmlAttribute(name="max_data_length")
    public String max_data_length;

    public Integer version;
    @XmlAttribute(name="stack_trace_enabled")
    public Boolean stack_trace_enabled;
    @XmlAttribute(name="cache_enabled")
    public Boolean cache_enabled;
    @XmlAttribute(name="cache_size")
    public Integer cache_size;
    @XmlAttribute(name="tight_encoding_enabled")
    public Boolean tight_encoding_enabled;
    @XmlAttribute(name="size_prefix_disabled")
    public Boolean size_prefix_disabled;
    @XmlAttribute(name="max_inactivity_duration")
    public Long max_inactivity_duration;
    @XmlAttribute(name="max_inactivity_duration_initial_delay")
    public Long max_inactivity_duration_initial_delay;
    @XmlAttribute(name="max_frame_size")
    public Long max_frame_size;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof OpenwireDTO)) return false;
        if (!super.equals(o)) return false;

        OpenwireDTO that = (OpenwireDTO) o;

        if (cache_enabled != null ? !cache_enabled.equals(that.cache_enabled) : that.cache_enabled != null)
            return false;
        if (cache_size != null ? !cache_size.equals(that.cache_size) : that.cache_size != null) return false;
        if (max_data_length != null ? !max_data_length.equals(that.max_data_length) : that.max_data_length != null)
            return false;
        if (max_frame_size != null ? !max_frame_size.equals(that.max_frame_size) : that.max_frame_size != null)
            return false;
        if (max_inactivity_duration != null ? !max_inactivity_duration.equals(that.max_inactivity_duration) : that.max_inactivity_duration != null)
            return false;
        if (max_inactivity_duration_initial_delay != null ? !max_inactivity_duration_initial_delay.equals(that.max_inactivity_duration_initial_delay) : that.max_inactivity_duration_initial_delay != null)
            return false;
        if (size_prefix_disabled != null ? !size_prefix_disabled.equals(that.size_prefix_disabled) : that.size_prefix_disabled != null)
            return false;
        if (stack_trace_enabled != null ? !stack_trace_enabled.equals(that.stack_trace_enabled) : that.stack_trace_enabled != null)
            return false;
        if (tight_encoding_enabled != null ? !tight_encoding_enabled.equals(that.tight_encoding_enabled) : that.tight_encoding_enabled != null)
            return false;
        if (version != null ? !version.equals(that.version) : that.version != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (max_data_length != null ? max_data_length.hashCode() : 0);
        result = 31 * result + (version != null ? version.hashCode() : 0);
        result = 31 * result + (stack_trace_enabled != null ? stack_trace_enabled.hashCode() : 0);
        result = 31 * result + (cache_enabled != null ? cache_enabled.hashCode() : 0);
        result = 31 * result + (cache_size != null ? cache_size.hashCode() : 0);
        result = 31 * result + (tight_encoding_enabled != null ? tight_encoding_enabled.hashCode() : 0);
        result = 31 * result + (size_prefix_disabled != null ? size_prefix_disabled.hashCode() : 0);
        result = 31 * result + (max_inactivity_duration != null ? max_inactivity_duration.hashCode() : 0);
        result = 31 * result + (max_inactivity_duration_initial_delay != null ? max_inactivity_duration_initial_delay.hashCode() : 0);
        result = 31 * result + (max_frame_size != null ? max_frame_size.hashCode() : 0);
        return result;
    }
}
