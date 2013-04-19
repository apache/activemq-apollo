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
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@XmlRootElement(name = "dispatch_queue_metrics")
@XmlAccessorType(XmlAccessType.FIELD)
@JsonIgnoreProperties(ignoreUnknown = true)
public class DispatchQueueMetrics {

    /**
     * How long the metrics gathered
     */
    @XmlAttribute(name="duration")
    public long duration;

    /**
     * The name of the dispatch queue associated with the metrics collected.
     */
    @XmlAttribute(name="queue")
    public String queue;

    /**
     * The number of tasks waiting to execute
     */
    @XmlAttribute(name="waiting")
    public long waiting;

    /**
     * The longest amount of time a task spent waiting to execute (in nanoseconds).
     */
    @XmlAttribute(name="wait_time_max")
    public long wait_time_max;

    /**
     * The total time all tasks spent waiting to execute (in nanoseconds).
     */
    @XmlAttribute(name="wait_time_total")
    public long wait_time_total;


    /**
     * The number of tasks executed.
     */
    @XmlAttribute(name="executed")
    public long executed;

    /**
     * The longest amount of time a task spent executing (in nanoseconds).
     */
    @XmlAttribute(name="execute_time_max")
    public long execute_time_max;

    /**
     *  The total time all tasks spent executing (in nanoseconds).
     */
    @XmlAttribute(name="execute_time_total")
    public long execute_time_total;



}