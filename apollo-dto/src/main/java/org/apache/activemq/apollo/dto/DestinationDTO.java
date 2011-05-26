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

import org.codehaus.jackson.annotate.JsonTypeInfo;

import javax.xml.bind.annotation.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@XmlType(name = "destination")
@XmlSeeAlso({QueueDestinationDTO.class, DurableSubscriptionDestinationDTO.class})
@JsonTypeInfo(use=JsonTypeInfo.Id.CLASS, include=JsonTypeInfo.As.PROPERTY, property="@class")
@XmlAccessorType(XmlAccessType.FIELD)
abstract public class DestinationDTO {

    @XmlElement(name = "path")
    public List<String> path = new ArrayList<String>();

    public DestinationDTO() {
    }

    public DestinationDTO(List<String> path) {
        this.path = path;
    }

    public DestinationDTO(String path[]) {
        this(Arrays.asList(path));
    }

    public String name(String separator) {
        StringBuilder sb  = new StringBuilder();
        for( String p : path) {
            if( sb.length() != 0 ) {
                sb.append(separator);
            }
            sb.append(p);
        }
        return sb.toString();
    }
}