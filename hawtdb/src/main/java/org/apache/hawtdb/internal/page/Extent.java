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

import javolution.io.Struct;

import org.apache.activemq.util.buffer.Buffer;
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
    
    public final static byte MAGIC_VALUE = 'x'; 
    
    static private class Header extends Struct {
        /**
         * A constant prefix that can be used to identify the page type.
         */
        public final Signed8 magic = new Signed8();
        /** 
         * The number of bytes in the extent, including the header.
         */
        public final Signed32 length = new Signed32();
        /**
         * The page of the next extent or -1.
         */
        public final Signed32 next = new Signed32();
    }

    private final Extent.Header header = new Header();

    private final Paged paged;
    private final int page;
    private ByteBuffer buffer;

    private int bufferStartPosition;

    public Extent(Paged paged, int page) {
        this.paged = paged;
        this.page = page;
    }
    
    @Override
    public String toString() {
        int position = 0;
        int limit=0;
        if( buffer!=null ) {
            position = buffer.position()-bufferStartPosition;
            limit = buffer.limit()-bufferStartPosition;
        }
        return "{ page: "+page+", position: "+position+", limit: "+limit+" }";
    }
    
    public void readOpen() {
        buffer = paged.slice(SliceType.READ, page, 1);
        header.setByteBuffer(buffer, buffer.position());
        if( header.magic.get() != MAGIC_VALUE ) {
            throw new IOPagingException("Invalid extent read request.  The requested page was not an extent: "+page);
        }
        
        int length = header.length.get();
        int pages = paged.pages(length);
        if( pages > 1 ) {
            paged.unslice(buffer);
            buffer = paged.slice(SliceType.READ, page, pages);
            header.setByteBuffer(buffer, buffer.position());
        }
        
        bufferStartPosition = buffer.position();
        buffer.position(bufferStartPosition+header.size());
        buffer.limit(bufferStartPosition+length);
    }

    public void writeOpen(short size) {
        buffer = paged.slice(SliceType.WRITE, page, size);
        header.setByteBuffer(buffer, buffer.position());
        bufferStartPosition = buffer.position();
        buffer.position(bufferStartPosition+header.size());
    }

    public void writeCloseLinked(int next) {
        int length = buffer.position()-bufferStartPosition;
        header.magic.set(MAGIC_VALUE);
        header.next.set(next);
        header.length.set(length);
        paged.unslice(buffer);
    }

    public void writeCloseEOF() {
        
        int length = buffer.position()-bufferStartPosition;
        header.magic.set(MAGIC_VALUE);
        header.next.set(-1);
        header.length.set(length);
        paged.unslice(buffer);

        int originalPages = paged.pages(buffer.limit()-bufferStartPosition);
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
        return header.next.get();
    }

    
    /**
     * Frees the linked extents at the provided page id.
     * 
     * @param paged
     * @param page
     */
    public static void freeLinked(Paged paged, int page) {
        ByteBuffer buffer = paged.slice(SliceType.READ, page, 1);
        Header header = new Header();
        header.setByteBuffer(buffer, buffer.position());
        if( header.magic.get() != MAGIC_VALUE ) {
            throw new IOPagingException("Invalid extent read request.  The requested page was not an extent: "+page);
        }
        int next = header.next.get();
        free(paged, next);
    }
    
    /**
     * Frees the extent at the provided page id.
     * 
     * @param paged
     * @param page
     */
    public static void free(Paged paged, int page) {
        while( page>=0 ) {
            ByteBuffer buffer = paged.slice(SliceType.READ, page, 1);
            try {
                Header header = new Header();
                header.setByteBuffer(buffer, buffer.position());
                if( header.magic.get() != MAGIC_VALUE ) {
                    throw new IOPagingException("Invalid extent read request.  The requested page was not an extent: "+page);
                }

                int next = header.next.get();
                paged.allocator().free(page, paged.pages(header.length.get()));
                page=next;
            } finally {
                paged.unslice(buffer);
            }
        }
    }

    /**
     * Un-frees the extent at the provided page id.  Basically undoes
     * a previous {@link #free(PageFile, int)} operation.
     * 
     * @param paged
     * @param page
     */
    public static void unfree(Paged paged, int page) {
        while( page>=0 ) {
            ByteBuffer buffer = paged.slice(SliceType.READ, page, 1);
            try {
                Header header = new Header();
                header.setByteBuffer(buffer, buffer.position());
                if( header.magic.get() != MAGIC_VALUE ) {
                    throw new IOPagingException("Invalid extent read request.  The requested page was not an extent: "+page);
                }
                
                int next = header.next.get();
                paged.allocator().unfree(page, paged.pages(header.length.get()));
                page=next;
            } finally {
                paged.unslice(buffer);
            }
        }
    }

    public int getPage() {
        return page;
    }
    
    public int getLength() {
        return header.length.get();
    }
}