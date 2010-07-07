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
import java.io.OutputStream;

import org.fusesource.hawtbuf.Buffer;
import org.apache.hawtdb.api.Paged;
import org.apache.hawtdb.internal.util.Ranges;


/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class ExtentOutputStream extends OutputStream {

    private static final short DEFAULT_EXTENT_SIZE = 128; // 128 * 4k = .5MB
    private final Paged paged;
    private final short extentSize;
    private final int page;
    private Extent current;
    private Ranges pages = new Ranges();
    
    
    public ExtentOutputStream(Paged paged) {
        this(paged, DEFAULT_EXTENT_SIZE);
    }
    
    public ExtentOutputStream(Paged paged, short extentSize) {
        this(paged, paged.allocator().alloc(extentSize), extentSize, extentSize);
    }
    
    public ExtentOutputStream(Paged paged, int page, short extentSize, short nextExtentSize ) {
        this.paged = paged;
        this.extentSize = nextExtentSize;
        this.page = page;
        current = new Extent(paged, page);
        current.writeOpen(extentSize);
    }

    @Override
    public String toString() {
        return "{ page: "+page+", extent size: "+extentSize+", current: "+current+" }";
    }
    
    public void write(int b) throws IOException {
        if (!current.write((byte) b)) {
            int nextPageId = this.paged.allocator().alloc(extentSize);
            current.writeCloseLinked(nextPageId);
            pages.add(current.getPage(), paged.pages(current.getLength()));
            current = new Extent(paged, nextPageId);
            current.writeOpen(extentSize);
            current.write((byte) b);
        }
    }

    @Override
    public void write(byte[] b, int off, int len) {
        Buffer buffer = new Buffer(b, off, len);
        while (buffer.length > 0) {
            if (!current.write(buffer)) {
                int nextPageId = this.paged.allocator().alloc(extentSize);
                current.writeCloseLinked(nextPageId);
                pages.add(current.getPage(), paged.pages(current.getLength()));
                current = new Extent(paged, nextPageId);
                current.writeOpen(extentSize);
            }
        }
    }

    public short getExtentSize() {
        return extentSize;
    }

    public int getPage() {
        return page;
    }

    @Override
    public void close(){
        current.writeCloseEOF();
        pages.add(current.getPage(), paged.pages(current.getLength()));
    }
    
    public Ranges getPages() {
        return pages;
    }
}
