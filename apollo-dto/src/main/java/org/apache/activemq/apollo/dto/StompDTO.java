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
 * Allow you to customize the stomp protocol implementation.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@XmlRootElement(name="stomp")
@XmlAccessorType(XmlAccessType.FIELD)
public class StompDTO extends ProtocolDTO {

    @XmlAttribute(name="add_user_header")
    public String add_user_header;

    /**
     * A broker accepts connections via it's configured connectors.
     */
    @XmlElement(name="add_user_header")
    public List<AddUserHeaderDTO> add_user_headers = new ArrayList<AddUserHeaderDTO>();

    @XmlAttribute(name="max_command_length")
    public Integer max_command_length;

    @XmlAttribute(name="max_header_length")
    public Integer max_header_length;

    @XmlAttribute(name="max_headers")
    public Integer max_headers;

    @XmlAttribute(name="max_data_length")
    public Integer max_data_length;

}
