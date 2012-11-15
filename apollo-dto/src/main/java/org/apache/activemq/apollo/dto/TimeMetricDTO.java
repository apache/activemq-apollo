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
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.concurrent.TimeUnit;

/**
 *
 *
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@XmlRootElement(name = "time_metric")
@XmlAccessorType(XmlAccessType.FIELD)
@JsonIgnoreProperties(ignoreUnknown = true)
public class TimeMetricDTO {

    /**
     * The number of timed events
     */
    @XmlAttribute
    public long count;

    /**
     * The total time in nanoseconds
     */
    @XmlAttribute
    public long total;

    /**
     * The maximum time in nanoseconds spent in an event
     */
    @XmlAttribute
    public long max;

    /**
     * The minimum time in nanoseconds spent in an event
     */
    @XmlAttribute
    public long min;


    public float max(TimeUnit unit) {
        return ((float)max) / unit.toNanos(1);
    }
    public float min(TimeUnit unit) {
        return ((float)min) / unit.toNanos(1);
    }
    public float total(TimeUnit unit) {
        return ((float)total) / unit.toNanos(1);
    }

    public float avg(TimeUnit unit) {
        return count==0 ? 0f : total(unit) / count;
    }

    public float frequency(TimeUnit unit) {
        return ((float)1) / avg(unit);
    }
    
}