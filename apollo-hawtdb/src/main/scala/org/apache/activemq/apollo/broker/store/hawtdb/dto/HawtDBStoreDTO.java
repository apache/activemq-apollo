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
@XmlRootElement(name="hawtdb_store")
@XmlAccessorType(XmlAccessType.FIELD)
public class HawtDBStoreDTO extends StoreDTO {

    @XmlAttribute
    public File directory;

    @XmlAttribute(name="archive_directory")
    public File archive_directory;

	@XmlAttribute(name="index_flush_interval")
	public Long index_flush_interval;

	@XmlAttribute(name="cleanup_interval")
	public Long cleanup_interval;

	@XmlAttribute(name="journal_log_size")
	public Integer journal_log_size;

    @XmlAttribute(name="journal_batch_size")
    public Integer journal_batch_size;

    @XmlAttribute(name="index_cache_size")
    public Integer index_cache_size;

    @XmlAttribute(name="index_page_size")
    public Short index_page_size;

    @XmlAttribute(name="fail_if_locked")
    public Boolean fail_if_locked;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        HawtDBStoreDTO that = (HawtDBStoreDTO) o;

        if (archive_directory != null ? !archive_directory.equals(that.archive_directory) : that.archive_directory != null)
            return false;
        if (cleanup_interval != null ? !cleanup_interval.equals(that.cleanup_interval) : that.cleanup_interval != null)
            return false;
        if (directory != null ? !directory.equals(that.directory) : that.directory != null) return false;
        if (fail_if_locked != null ? !fail_if_locked.equals(that.fail_if_locked) : that.fail_if_locked != null)
            return false;
        if (index_cache_size != null ? !index_cache_size.equals(that.index_cache_size) : that.index_cache_size != null)
            return false;
        if (index_flush_interval != null ? !index_flush_interval.equals(that.index_flush_interval) : that.index_flush_interval != null)
            return false;
        if (index_page_size != null ? !index_page_size.equals(that.index_page_size) : that.index_page_size != null)
            return false;
        if (journal_batch_size != null ? !journal_batch_size.equals(that.journal_batch_size) : that.journal_batch_size != null)
            return false;
        if (journal_log_size != null ? !journal_log_size.equals(that.journal_log_size) : that.journal_log_size != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (directory != null ? directory.hashCode() : 0);
        result = 31 * result + (archive_directory != null ? archive_directory.hashCode() : 0);
        result = 31 * result + (index_flush_interval != null ? index_flush_interval.hashCode() : 0);
        result = 31 * result + (cleanup_interval != null ? cleanup_interval.hashCode() : 0);
        result = 31 * result + (journal_log_size != null ? journal_log_size.hashCode() : 0);
        result = 31 * result + (journal_batch_size != null ? journal_batch_size.hashCode() : 0);
        result = 31 * result + (index_cache_size != null ? index_cache_size.hashCode() : 0);
        result = 31 * result + (index_page_size != null ? index_page_size.hashCode() : 0);
        result = 31 * result + (fail_if_locked != null ? fail_if_locked.hashCode() : 0);
        return result;
    }
}
