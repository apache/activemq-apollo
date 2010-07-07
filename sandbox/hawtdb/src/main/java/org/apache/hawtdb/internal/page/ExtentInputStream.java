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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import org.fusesource.hawtbuf.Buffer;
import org.apache.hawtdb.api.Paged;
import org.apache.hawtdb.internal.util.Ranges;


/**
 * An InputStream which reads it's data from an 
 * extent previously written with the {@link ExtentOutputStream}.
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class ExtentInputStream extends InputStream {

    private final Paged paged;
    private Extent current;
    private final int page;
    private Ranges pages = new Ranges();

    public ExtentInputStream(Paged paged, int page) {
        this.paged = paged;
        this.page = page;
        current = new Extent(paged, page);
        current.readOpen();
        pages.add(current.getPage(), paged.pages(current.getLength()));
    }
    
    @Override
    public String toString() {
        return "{ page: "+page+", current: "+current+" }";
    }
    

    @Override
    public int read() throws IOException {
        if( current == null ) {
            return -1;
        }
        if (current.atEnd()) {
            int next = current.getNext();
            if (next == -1) {
                current.readClose();
                current=null;
                return -1;
            }
            current.readClose();
            current = new Extent(paged, next);
            current.readOpen();
            pages.add(current.getPage(), paged.pages(current.getLength()));
        }
        return current.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int rc=len;
        Buffer buffer = new Buffer(b, off, len);
        if( current == null ) {
            throw new EOFException();
        }
        while (buffer.length > 0) {
            if (current.atEnd()) {
                int next = current.getNext();
                if (next == -1) {
                    current.readClose();
                    current=null;
                    break;
                }
                current.readClose();
                current = new Extent(paged, next);
                current.readOpen();
                pages.add(current.getPage(), paged.pages(current.getLength()));
            }
            current.read(buffer);
        }
        rc-=buffer.length;
        if ( rc==0 ) {
            throw new EOFException();
        }
        return rc;
    }

    @Override
    public void close() throws IOException {
        if( current!=null ) {
            current.readClose();
            current=null;
        }
    }
    
    public Ranges getPages() {
        return pages;
    }
}
