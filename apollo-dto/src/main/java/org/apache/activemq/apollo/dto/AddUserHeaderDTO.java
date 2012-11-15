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
import javax.xml.bind.annotation.XmlValue;

/**
 * <p>
 * This class is used to more finely control how user headers are added to received STOMP messages.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@XmlAccessorType(XmlAccessType.FIELD)
@JsonIgnoreProperties(ignoreUnknown = true)
public class AddUserHeaderDTO {

    /**
     * The name of the header to set
     */
    @XmlValue
    public String name;

    /**
     * If the user has multiple principals which match
     * then they will all be listed in the value of the header
     * entry separated by the configured separator value.  If the
     * separator is not set, then only the first matching principal
     * will be used in the value of the header entry.
     */
    @XmlAttribute(name="separator")
    public String separator;

    /**
     * The user sending the message may have many principals,
     * setting the kind will only select those principals who's
     * class name matches the kind.  Defaults to *.
     */
    @XmlAttribute(name="kind")
    public String kind;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AddUserHeaderDTO)) return false;

        AddUserHeaderDTO that = (AddUserHeaderDTO) o;

        if (kind != null ? !kind.equals(that.kind) : that.kind != null) return false;
        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        if (separator != null ? !separator.equals(that.separator) : that.separator != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (separator != null ? separator.hashCode() : 0);
        result = 31 * result + (kind != null ? kind.hashCode() : 0);
        return result;
    }
}
