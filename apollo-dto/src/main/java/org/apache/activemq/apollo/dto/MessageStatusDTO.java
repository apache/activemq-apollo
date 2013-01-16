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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class MessageStatusDTO {

    /**
     * Additional
     */
    public EntryStatusDTO entry;

    /**
     * When the message will expire
     */
    public long expiration;

    /**
     * Is the delivery persistent?
     */
    public boolean persistent = false;

    /**
     * The encoding that the message is stored in.
     */
    public String codec;

    /**
     * A map of all the headers in the mesasge.
     */
    public HashMap<String, Object> headers = new HashMap<String, Object>();

    /**
     * The body of the message in base 64 encoding.
     */
    public String base64_body;

    /**
     * Has the body been truncated.
     */
    public boolean body_truncated;

}