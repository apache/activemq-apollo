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

    @XmlAttribute(name="fail_if_locked")
    public Boolean fail_if_locked;

    @XmlAttribute(name="gc_interval")
    public Integer gc_interval;

    @XmlAttribute(name="read_threads")
    public Integer read_threads;

    @XmlAttribute(name="verify_checksums")
    public Boolean verify_checksums;

    @XmlAttribute(name="log_size")
    public Integer log_size;

    @XmlAttribute(name="log_write_buffer_size")
    public Integer log_write_buffer_size;

    @XmlAttribute(name="index_page_size")
    public Integer index_page_size;

    @XmlAttribute(name="index_cache_size")
    public Long index_cache_size;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof HawtDBStoreDTO)) return false;
        if (!super.equals(o)) return false;

        HawtDBStoreDTO that = (HawtDBStoreDTO) o;

        if (directory != null ? !directory.equals(that.directory) : that.directory != null)
            return false;
        if (fail_if_locked != null ? !fail_if_locked.equals(that.fail_if_locked) : that.fail_if_locked != null)
            return false;
        if (gc_interval != null ? !gc_interval.equals(that.gc_interval) : that.gc_interval != null)
            return false;
        if (index_cache_size != null ? !index_cache_size.equals(that.index_cache_size) : that.index_cache_size != null)
            return false;
        if (index_page_size != null ? !index_page_size.equals(that.index_page_size) : that.index_page_size != null)
            return false;
        if (log_size != null ? !log_size.equals(that.log_size) : that.log_size != null)
            return false;
        if (log_write_buffer_size != null ? !log_write_buffer_size.equals(that.log_write_buffer_size) : that.log_write_buffer_size != null)
            return false;
        if (read_threads != null ? !read_threads.equals(that.read_threads) : that.read_threads != null)
            return false;
        if (verify_checksums != null ? !verify_checksums.equals(that.verify_checksums) : that.verify_checksums != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (directory != null ? directory.hashCode() : 0);
        result = 31 * result + (fail_if_locked != null ? fail_if_locked.hashCode() : 0);
        result = 31 * result + (gc_interval != null ? gc_interval.hashCode() : 0);
        result = 31 * result + (read_threads != null ? read_threads.hashCode() : 0);
        result = 31 * result + (verify_checksums != null ? verify_checksums.hashCode() : 0);
        result = 31 * result + (log_size != null ? log_size.hashCode() : 0);
        result = 31 * result + (log_write_buffer_size != null ? log_write_buffer_size.hashCode() : 0);
        result = 31 * result + (index_page_size != null ? index_page_size.hashCode() : 0);
        result = 31 * result + (index_cache_size != null ? index_cache_size.hashCode() : 0);
        return result;
    }
}
