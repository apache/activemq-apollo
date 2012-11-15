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

import javax.xml.bind.annotation.*;

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@XmlRootElement(name="jvm_metrics")
@XmlAccessorType(XmlAccessType.FIELD)
@JsonIgnoreProperties(ignoreUnknown = true)
public class JvmMetricsDTO {

    @XmlElement(name="heap_memory")
    public MemoryMetricsDTO heap_memory;

    @XmlElement(name="non_heap_memory")
    public MemoryMetricsDTO non_heap_memory;

    @XmlAttribute(name="classes_loaded")
    public int classes_loaded;

    @XmlAttribute(name="classes_unloaded")
    public long classes_unloaded;

    @XmlAttribute(name="threads_current")
    public int threads_current;

    @XmlAttribute(name="threads_peak")
    public int threads_peak;

    @XmlAttribute(name="os_arch")
    public String os_arch;

    @XmlAttribute(name="os_name")
    public String os_name;

    @XmlAttribute(name="os_memory_total")
    public long os_memory_total;

    @XmlAttribute(name="os_memory_free")
    public long os_memory_free;

    @XmlAttribute(name="os_swap_total")
    public long os_swap_total;

    @XmlAttribute(name="os_swap_free")
    public long os_swap_free;

    @XmlAttribute(name="os_fd_open")
    public long os_fd_open;

    @XmlAttribute(name="os_fd_max")
    public long os_fd_max;

    @XmlAttribute(name="os_load_average")
    public double os_load_average;

    @XmlAttribute(name="os_cpu_time")
    public long os_cpu_time;

    @XmlAttribute(name="os_processors")
    public int os_processors;

    @XmlAttribute(name="runtime_name")
    public String runtime_name;

    @XmlAttribute(name="jvm_name")
    public String jvm_name;

    @XmlAttribute(name="uptime")
    public long uptime;

    @XmlAttribute(name="start_time")
    public long start_time;

}