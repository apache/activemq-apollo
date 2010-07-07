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

    @XmlAttribute(name="directory", required=false)
    public File directory;

    @XmlAttribute(name="archive-directory", required=false)
    public File archiveDirectory;

	@XmlAttribute(name="index-flush-interval", required=false)
	public long indexFlushInterval = 5 * 1000L;

	@XmlAttribute(name="cleanup-interval", required=false)
	public long cleanupInterval = 30 * 1000L;

	@XmlAttribute(name="journal-log-size", required=false)
	public int journalLogSize = 1024*1024*64;

    @XmlAttribute(name="journal-batch-size", required=false)
    public int journalBatchSize = 1024*256;

    @XmlAttribute(name="index-cache-size", required=false)
    public int indexCacheSize = 5000;

    @XmlAttribute(name="index-page-size", required=false)
    public short indexPageSize = 512;

    @XmlAttribute(name="fail-if-locked", required=false)
    public boolean failIfLocked = false;


}
