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
@XmlRootElement(name = "queue")
@XmlAccessorType(XmlAccessType.FIELD)
@JsonIgnoreProperties(ignoreUnknown = true)
public class QueueDTO extends QueueSettingsDTO {

    /**
     * A unique id of queue
     */
	@XmlAttribute
	public String id;

    /**
     * Controls when the queue will auto delete.
     * If set to zero, then the queue will NOT auto
     * delete, otherwise the queue will auto delete
     * after it has been unused for the number
     * of seconds configured in this field.  If unset,
     * it defaults to 5 minutes.
     */
    @XmlAttribute(name="auto_delete_after")
    public Integer auto_delete_after;

    /**
     * If set to true, then once the queue
     * is created all messages sent to the queue
     * will be mirrored to a topic of the same name
     * and all messages sent to the topic will be mirror
     * to the queue.
     */
    @XmlAttribute
    public Boolean mirrored;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof QueueDTO)) return false;
        if (!super.equals(o)) return false;

        QueueDTO queueDTO = (QueueDTO) o;

        if (auto_delete_after != null ? !auto_delete_after.equals(queueDTO.auto_delete_after) : queueDTO.auto_delete_after != null)
            return false;
        if (id != null ? !id.equals(queueDTO.id) : queueDTO.id != null) return false;
        if (mirrored != null ? !mirrored.equals(queueDTO.mirrored) : queueDTO.mirrored != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (id != null ? id.hashCode() : 0);
        result = 31 * result + (auto_delete_after != null ? auto_delete_after.hashCode() : 0);
        result = 31 * result + (mirrored != null ? mirrored.hashCode() : 0);
        return result;
    }
}
