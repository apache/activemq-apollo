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

import javax.xml.bind.annotation.*;
import java.util.ArrayList;
import java.util.List;

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@XmlRootElement(name="broker-status")
@XmlAccessorType(XmlAccessType.FIELD)
public class BrokerStatusDTO extends ServiceStatusDTO {

    /**
     * The current time on the broker machine.  In milliseconds since the epoch.
     */
	@XmlAttribute(name="current-time")
	public long currentTime;

    /**
     * Ids of all the virtual hosts running on the broker
     */
    @XmlElement(name="virtual-host")
    public List<Long> virtualHosts = new ArrayList<Long>();

    /**
     * Ids of all the connections running on the broker
     */
    @XmlElement(name="connectors")
    public List<Long> connectors = new ArrayList<Long>();

    /**
     * The current running configuration of the object
     */
    @XmlElement(name="config")
    public BrokerDTO config = null;

}
