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
package org.apache.activemq.apollo.broker.store.hawtdb.dto;

import org.apache.activemq.apollo.dto.StoreDTO;

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

    @XmlAttribute
    public File directory;

    @XmlAttribute(name="archive-directory")
    public File archive_directory;

	@XmlAttribute(name="index-flush-interval")
	public Long index_flush_interval;

	@XmlAttribute(name="cleanup-interval")
	public Long cleanup_interval;

	@XmlAttribute(name="journal-log-size")
	public Integer journal_log_size;

    @XmlAttribute(name="journal-batch-size")
    public Integer journal_batch_size;

    @XmlAttribute(name="index-cache-size")
    public Integer index_cache_size;

    @XmlAttribute(name="index-page-size")
    public Short index_page_size;

    @XmlAttribute(name="fail-if-locked")
    public Boolean fail_if_locked;


}
