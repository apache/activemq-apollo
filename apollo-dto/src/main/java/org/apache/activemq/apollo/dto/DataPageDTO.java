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
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@XmlRootElement(name="data_table")
@XmlAccessorType(XmlAccessType.FIELD)
@JsonIgnoreProperties(ignoreUnknown = true)
public class DataPageDTO {

    /**
     */
    @XmlAttribute(name="page")
    public int page;

    /**
     */
    @XmlAttribute(name="page_size")
    public int page_size;

    /**
     */
    @XmlAttribute(name="total_pages")
    public int total_pages;

    /**
     */
    @XmlAttribute(name="total_rows")
    public long total_rows;

    @XmlElement(name="header")
    public List<String> headers = new ArrayList<String>();

    @XmlElement(name="row")
    public List<?> rows = new ArrayList<Object>();

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DataPageDTO)) return false;

        DataPageDTO that = (DataPageDTO) o;

        if (page != that.page) return false;
        if (page_size != that.page_size) return false;
        if (total_pages != that.total_pages) return false;
        if (total_rows != that.total_rows) return false;
        if (headers != null ? !headers.equals(that.headers) : that.headers != null) return false;
        if (rows != null ? !rows.equals(that.rows) : that.rows != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = page;
        result = 31 * result + page_size;
        result = 31 * result + total_pages;
        result = 31 * result + (int) (total_rows ^ (total_rows >>> 32));
        result = 31 * result + (headers != null ? headers.hashCode() : 0);
        result = 31 * result + (rows != null ? rows.hashCode() : 0);
        return result;
    }
}
