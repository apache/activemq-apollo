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
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@XmlRootElement(name="auto_gc")
@XmlAccessorType(XmlAccessType.FIELD)
@JsonIgnoreProperties(ignoreUnknown = true)
public class AutoGCServiceDTO extends CustomServiceDTO {

    /**
     * How often to force a GC in seconds.  Defaults to 30.
     */
    @XmlAttribute
    public Integer interval;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AutoGCServiceDTO)) return false;
        if (!super.equals(o)) return false;

        AutoGCServiceDTO that = (AutoGCServiceDTO) o;

        if (interval != null ? !interval.equals(that.interval) : that.interval != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (interval != null ? interval.hashCode() : 0);
        return result;
    }
}