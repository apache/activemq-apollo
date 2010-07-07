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

import org.codehaus.jackson.annotate.JsonProperty;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.File;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@XmlRootElement(name="hawtdb-store")
@XmlAccessorType(XmlAccessType.FIELD)
public class HawtDBStoreDTO extends StoreDTO {

    @JsonProperty
    @XmlAttribute
    public File directory;

    @JsonProperty("archive_directory")
    @XmlAttribute(name="archive-directory")
    public File archiveDirectory;

    @JsonProperty("index_flush_interval")
	@XmlAttribute(name="index-flush-interval")
	public long indexFlushInterval = 5 * 1000L;

    @JsonProperty("cleanup_interval")
	@XmlAttribute(name="cleanup-interval")
	public long cleanupInterval = 30 * 1000L;

    @JsonProperty("journal_log_size")
	@XmlAttribute(name="journal-log-size")
	public int journalLogSize = 1024*1024*64;

    @JsonProperty("journal_batch_size")
    @XmlAttribute(name="journal-batch-size")
    public int journalBatchSize = 1024*256;

    @JsonProperty("index_cache_size")
    @XmlAttribute(name="index-cache-size")
    public int indexCacheSize = 5000;

    @JsonProperty("index_page_size")
    @XmlAttribute(name="index-page-size")
    public short indexPageSize = 512;

    @JsonProperty("fail_if_locked")
    @XmlAttribute(name="fail-if-locked")
    public boolean failIfLocked = false;


}
