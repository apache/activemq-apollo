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

	@XmlAttribute(name="checkpoint-interval", required=false)
	public Long checkpointInterval;
	@XmlAttribute(name="cleanup-interval", required=false)
	public Long cleanupInterval;
	@XmlAttribute(name="purge-on-startup", required=false)
	public Boolean purgeOnStartup;
	@XmlAttribute(name="index-write-async", required=false)
	public Boolean indexWriteAsync;
	@XmlAttribute(name="journal-disk-syncs", required=false)
	public Boolean journalDiskSyncs;
	@XmlAttribute(name="fail-if-database-is-locked", required=false)
	public Boolean failIfDatabaseIsLocked;
	@XmlAttribute(name="index-write-batch-size", required=false)
	public Integer indexWriteBatchSize;
	@XmlAttribute(name="journal-max-file-length", required=false)
	public Integer journalMaxFileLength;
	@XmlAttribute(name="directory", required=false)
	public File directory;


}
