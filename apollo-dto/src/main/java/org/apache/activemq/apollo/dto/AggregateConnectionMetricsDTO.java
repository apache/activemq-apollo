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
 * Collects aggregated metrics on all connections.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@XmlRootElement(name="aggregate_connection_metrics")
@XmlAccessorType(XmlAccessType.FIELD)
@JsonIgnoreProperties(ignoreUnknown = true)
public class AggregateConnectionMetricsDTO {

    /**
     * The current time on the broker machine.  In milliseconds since the epoch.
     */
	@XmlAttribute(name="current_time")
	public long current_time;

    /**
     * Total number of bytes that have been read from the connections.
     */
	@XmlAttribute(name="read_counter")
	public long read_counter;

    /**
     * Total number of bytes that have been written to the connections.
     */
	@XmlAttribute(name="write_counter")
	public long write_counter;

    /**
     * Total number of subscriptions opened by the connections.
     */
    @XmlAttribute(name="subscription_count")
	public long subscription_count;

    /**
     * Total number of opened connections.
     */
    @XmlAttribute(name="objects")
	public long objects;

}
