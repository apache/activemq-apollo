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
import java.util.List;

/**
 * <p>Holds a page of data browsed from a destination.</p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@XmlRootElement(name="browse_page")
@XmlAccessorType(XmlAccessType.FIELD)
@JsonIgnoreProperties(ignoreUnknown = true)
public class BrowsePageDTO {

    /**
     */
    @XmlAttribute(name="first_seq")
    public long first_seq;

    /**
     */
    @XmlAttribute(name="last_seq")
    public long last_seq;

    /**
     */
    @XmlAttribute(name="total_messages")
    public long total_messages;

    @XmlElement(name="messages")
    public List<MessageStatusDTO> messages = new ArrayList<MessageStatusDTO>();

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BrowsePageDTO)) return false;

        BrowsePageDTO that = (BrowsePageDTO) o;

        if (first_seq != that.first_seq) return false;
        if (last_seq != that.last_seq) return false;
        if (total_messages != that.total_messages) return false;
        if (messages != null ? !messages.equals(that.messages) : that.messages != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (first_seq ^ (first_seq >>> 32));
        result = 31 * result + (int) (last_seq ^ (last_seq >>> 32));
        result = 31 * result + (int) (total_messages ^ (total_messages >>> 32));
        result = 31 * result + (messages != null ? messages.hashCode() : 0);
        return result;
    }
}
