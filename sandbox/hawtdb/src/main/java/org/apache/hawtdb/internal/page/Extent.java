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

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.List;

import org.fusesource.hawtbuf.buffer.Buffer;
import org.apache.hawtdb.api.IOPagingException;
import org.apache.hawtdb.api.Paged;
import org.apache.hawtdb.api.Paged.SliceType;


/**
 * An extent is a sequence of adjacent pages which can be linked
 * to subsequent extents.
 * 
 * Extents allow you to write large streams of data to a Paged object
 * contiguously to avoid fragmentation.
 * 
 * The first page of the extent contains a header which specifies
 * the size of the extent and the page id of the next extent that
 * it is linked to.
 *  
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class Extent {
    
    public final static Buffer DEFAULT_MAGIC = new Buffer(new byte[]{'x'}); 

    private final Paged paged;
    private final int page;
    private final Buffer magic;

    private ByteBuffer buffer;
    
    private int length;
    private int next;
    
    public Extent(Paged paged, int page) {
        this(paged, page, DEFAULT_MAGIC);
    }
    
    public Extent(Paged paged, int page, Buffer magic) {
        this.paged = paged;
        this.page = page;
        this.magic = magic;
    }
    
    @Override
    public String toString() {
        Integer position = null;
        Integer limit = null;
        if( buffer!=null ) {
            position = buffer.position();
            limit = buffer.limit();
        }
        return "{ page: "+page+", position: "+position+", limit: "+limit+", length: "+length+", next: "+next+" }";
    }
    
    
    public void readHeader() {
        buffer = paged.slice(SliceType.READ, page, 1);
        
        Buffer m = new Buffer(magic.length);
        buffer.get(m.data);
        
        if( !magic.equals(m) ) {
            throw new IOPagingException("Invalid extent read request.  The requested page was not an extent: "+page);
        }
        
        IntBuffer ib = buffer.asIntBuffer();
        length = ib.get();
        next = ib.get();
    }
    
    public void readOpen() {
        readHeader();
        int pages = paged.pages(length);
        if( pages > 1 ) {
            paged.unslice(buffer);
            buffer = paged.slice(SliceType.READ, page, pages);
        }
        buffer.position(magic.length+8);
        buffer.limit(length);
    }

    public void writeOpen(short size) {
        buffer = paged.slice(SliceType.WRITE, page, size);
        buffer.position(magic.length+8);
    }

    public int writeCloseLinked(int next) {
        this.next = next;
        length = buffer.position();
        buffer.position(0);
        buffer.put(magic.data, magic.offset, magic.length);
        IntBuffer ib = buffer.asIntBuffer();
        ib.put(length);
        ib.put(next);
        paged.unslice(buffer);
        return length;
    }

    public void writeCloseEOF() {
        int length = writeCloseLinked(-1);
        int originalPages = paged.pages(buffer.limit());
        int usedPages = paged.pages(length);
        int remainingPages = originalPages-usedPages;
        
        // Release un-used pages.
        if (remainingPages>0) {
            paged.allocator().free(page+usedPages, remainingPages);
        }
        paged.unslice(buffer);
    }
    
    public void readClose() {
        paged.unslice(buffer);
    }

    boolean atEnd() {
        return buffer.remaining() == 0;
    }

    /**
     * @return true if the write fit into the extent.
     */
    public boolean write(byte b) {
        if (atEnd()) {
            return false;
        }
        buffer.put(b);
        return true;
    }

    public boolean write(Buffer source) {
        while (source.length > 0) {
            if (atEnd()) {
                return false;
            }
            int count = Math.min(buffer.remaining(), source.length);
            buffer.put(source.data, source.offset, count);
            source.offset += count;
            source.length -= count;
        }
        return true;
    }

    public int read() {
        return buffer.get() & 0xFF;
    }

    public void read(Buffer target) {
        while (target.length > 0 && !atEnd()) {
            int count = Math.min(buffer.remaining(), target.length);
            buffer.get(target.data, target.offset, count);
            target.offset += count;
            target.length -= count;
        }
    }

    public int getNext() {
        return next;
    }
    
    /**
     * Frees the linked extents at the provided page id.
     * 
     * @param paged
     * @param page
     */
    public static List<Integer> freeLinked(Paged paged, int page) {
        return freeLinked(paged, page, DEFAULT_MAGIC);
    }
    
    public static List<Integer> freeLinked(Paged paged, int page, Buffer magic) {
        Extent extent = new Extent(paged, page, magic);
        extent.readHeader();
        return free(paged, extent.getNext());
    }    
    
    /**
     * Frees the extent at the provided page id.
     * 
     * @param paged
     * @param page
     */
    public static List<Integer> free(Paged paged, int page) {
        return free(paged, page, DEFAULT_MAGIC);
    }    
    public static List<Integer> free(Paged paged, int page, Buffer magic) {
        ArrayList<Integer> rc = new ArrayList<Integer>();
        while( page>=0 ) {
            Extent extent = new Extent(paged, page, magic);
            extent.readHeader();
            try {
                int pagesInExtent = paged.pages(extent.getLength());
                paged.allocator().free(page, pagesInExtent);
                for( int i=0; i < pagesInExtent; i++) {
                    rc.add(page+i);
                }
                page=extent.getNext();
            } finally {
                extent.readClose();
            }
        }
        return rc;
    }

    /**
     * Un-frees the extent at the provided page id.  Basically undoes
     * a previous {@link #free(PageFile, int)} operation.
     * 
     * @param paged
     * @param page
     */
    public static void unfree(Paged paged, int page) {
        unfree(paged, page, DEFAULT_MAGIC);
    }
    public static void unfree(Paged paged, int page, Buffer magic) {
        while( page>=0 ) {
            Extent extent = new Extent(paged, page, magic);
            extent.readHeader();
            try {
                paged.allocator().unfree(page, paged.pages(extent.length));
                page=extent.next;
            } finally {
                extent.readClose();
            }
        }
    }

    public int getPage() {
        return page;
    }
    
    public int getLength() {
        return length;
    }
}