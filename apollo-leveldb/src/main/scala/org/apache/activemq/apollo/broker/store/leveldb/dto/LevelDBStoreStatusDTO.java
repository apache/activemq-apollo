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
package org.apache.activemq.apollo.broker.store.leveldb.dto;

import org.apache.activemq.apollo.dto.IntMetricDTO;
import org.apache.activemq.apollo.dto.StoreStatusDTO;
import org.apache.activemq.apollo.dto.TimeMetricDTO;
import org.apache.activemq.apollo.dto.WebAdminDTO;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@XmlRootElement(name="leveldb_store_status")
@XmlAccessorType(XmlAccessType.FIELD)
@JsonIgnoreProperties(ignoreUnknown = true)
public class LevelDBStoreStatusDTO extends StoreStatusDTO {

    @XmlElement(name="journal_append_latency")
    public TimeMetricDTO journal_append_latency;

    @XmlElement(name="index_update_latency")
    public TimeMetricDTO index_update_latency;

    @XmlElement(name="message_load_batch_size")
    public IntMetricDTO message_load_batch_size;

    @XmlElement(name="leveldb_stats")
    public String index_stats;

    @XmlElement(name="last_checkpoint_pos")
    public long index_snapshot_pos;

    @XmlElement(name="last_append_pos")
    public long log_append_pos;

    @XmlElement(name="log_stats")
    public String log_stats;

}
