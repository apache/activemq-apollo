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
package org.apache.hawtdb.internal.index;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import org.apache.hawtdb.api.EncoderDecoder;
import org.apache.hawtdb.api.Index;
import org.apache.hawtdb.api.Paged;
import org.apache.hawtdb.api.Paged.SliceType;

import javolution.io.Struct;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class HashBins {
    
    public static final byte MAGIC [] = createMagic();
    
    static private class Header extends Struct {
        public final UTF8String magic = new UTF8String(4);
        public final Signed32 page = new Signed32();
        public final Signed32 capacity = new Signed32();
        public final Signed32 size = new Signed32();
        public final Signed32 active = new Signed32();
        
        static Header create(ByteBuffer buffer) {
            Header header = new Header();
            header.setByteBuffer(buffer, buffer.position());
            return header;
        }
    }

    static public final EncoderDecoder<HashBins> ENCODER_DECODER = new EncoderDecoder<HashBins>() {
        
        public HashBins load(Paged paged, int page) {
            ByteBuffer slice = paged.slice(SliceType.READ, page, 1);
            try {
                Header header = Header.create(slice);
                HashBins rc = new HashBins();
                rc.page = header.page.get();
                rc.capacity = header.capacity.get();
                rc.size = header.size.get();
                rc.active = header.active.get();
                return rc;
            } finally {
                paged.unslice(slice);
            }
        }

        public List<Integer> store(Paged paged, int page, HashBins object) {
            ByteBuffer slice = paged.slice(SliceType.WRITE, page, 1);
            try {
                Header header = Header.create(slice);
                header.magic.set("HASH");
                header.page.set(object.page);
                header.capacity.set(object.capacity);
                header.size.set(object.size);
                header.active.set(object.active);
                return Collections.emptyList();
            } finally {
                paged.unslice(slice);
            }
        }
        
        public void remove(Paged paged, int page) {
        }

    };
    
    
    int page=-1;
    int active;
    int capacity;
    int size;
    
    <Key,Value> void create(HashIndex<Key,Value> index, int capacity) {
        this.size = 0;
        this.active = 0;
        this.capacity = capacity;
        this.page = index.paged.allocator().alloc(capacity);
        for (int i = 0; i < capacity; i++) {
            index.BIN_FACTORY.create(index.paged, (page + i));
        }
        index.paged.put(ENCODER_DECODER, index.page, this);
    }
    
    <Key,Value> Value put(HashIndex<Key,Value> index, Key key, Value value) {
        Index<Key, Value> bin = bin(index, key);

        int originalSize = bin.size();
        Value result = bin.put(key,value);
        int newSize = bin.size();

        if (newSize != originalSize) {
            size++;
            if (newSize == 1) {
                active++;
            }
            index.paged.put(ENCODER_DECODER, index.page, this);
        }
        return result;
    }
    
    <Key,Value> Value remove(HashIndex<Key,Value> index, Key key) {
        Index<Key, Value> bin = bin(index, key);
        int originalSize = bin.size();
        Value result = bin.remove(key);
        int newSize = bin.size();
        
        if (newSize != originalSize) {
            size--;
            if (newSize == 0) {
                active--;
            }
            index.paged.put(ENCODER_DECODER, index.page, this);
        }
        return result;
    }
    
    <Key,Value> void clear(HashIndex<Key,Value> index) {
        for (int i = 0; i < capacity; i++) {
            bin(index, i).clear();
        }
        size = 0;
        active = 0;
    }
    
    <Key,Value> void destroy(HashIndex<Key,Value> index) {
        for (int i = 0; i < capacity; i++) {
            bin(index, i).clear();
        }
        index.paged.allocator().free(page, capacity);
        size = 0;
        active = 0;
        capacity = 0;
        page = -1;
    }

    <Key,Value> Index<Key,Value> bin(HashIndex<Key,Value> index, int bin) {
        return index.BIN_FACTORY.open(index.paged, page+bin);
    }

    <Key,Value> Index<Key,Value> bin(HashIndex<Key,Value> index, Key key) {
        int i = index(key);
        return index.BIN_FACTORY.open(index.paged, page+i);
    }

    <Key,Value> int index(Key x) {
        try {
            return Math.abs(x.hashCode()%capacity);
        } catch (ArithmeticException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            throw e;
        }
    }
    
    @Override
    public String toString() {
        return "{ page:"+page+", capacity: "+capacity+", active: "+active+", size: "+size+" }";
    }
    
    private static byte[] createMagic() {
        try {
            return "HASH".getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    
}