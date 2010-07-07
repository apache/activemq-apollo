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

    @XmlAttribute
    public File directory;

    @XmlAttribute(name="archive-directory")
    public File archive_directory;

	@XmlAttribute(name="index-flush-interval")
	public long index_flush_interval = 5 * 1000L;

	@XmlAttribute(name="cleanup-interval")
	public long cleanup_interval = 30 * 1000L;

	@XmlAttribute(name="journal-log-size")
	public int journal_log_size = 1024*1024*64;

    @XmlAttribute(name="journal-batch-size")
    public int journal_batch_size = 1024*256;

    @XmlAttribute(name="index-cache-size")
    public int index_cache_size = 5000;

    @XmlAttribute(name="index-page-size")
    public short index_page_size = 512;

    @XmlAttribute(name="fail-if-locked")
    public boolean fail_if_locked = false;


}
