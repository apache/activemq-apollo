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
package org.apache.hawtdb.internal.page;

import java.io.IOException;

import org.apache.hawtdb.api.IOPagingException;
import org.apache.hawtdb.internal.io.MemoryMappedFileFactory;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class PageFileFactory extends MemoryMappedFileFactory {

    private PageFile pageFile;

    protected int headerSize = 0;
    protected short pageSize = 1024 * 4;
    protected int maxPages = Integer.MAX_VALUE;

    public PageFile getPageFile() {
        return pageFile;
    }

    public void open() {
        try {
            super.open();
        } catch (IOException e) {
            throw new IOPagingException(e);
        }
        if (pageFile == null) {
            if( pageSize <= 0 ) {
                throw new IllegalArgumentException("pageSize property must be greater than 0");
            }
            if( maxPages <= 0 ) {
                throw new IllegalArgumentException("maxPages property must be greater than 0");
            }
            if( headerSize < 0 ) {
                throw new IllegalArgumentException("headerSize property cannot be negative.");
            }
            try {
                pageFile = new PageFile(getMemoryMappedFile(), pageSize, headerSize, maxPages);
            } catch (IOException e) {
                throw new IOPagingException(e);
            }
        }
    } 
    
    public void close() {
        if (pageFile != null) {
            pageFile = null;
        }        
        super.close();
    }

    public int getHeaderSize() {
        return headerSize;
    }
    public void setHeaderSize(int headerSize) {
        this.headerSize = headerSize;
    }

    public short getPageSize() {
        return pageSize;
    }
    public void setPageSize(short pageSize) {
        this.pageSize = pageSize;
    }

    public int getMaxPages() {
        return maxPages;
    }
    public void setMaxPages(int maxPages) {
        this.maxPages = maxPages;
    }
    public void setMaxFileSize(long size) {
        setMaxPages( (int)((size-getHeaderSize())/getPageSize()) );
    }
    
}
