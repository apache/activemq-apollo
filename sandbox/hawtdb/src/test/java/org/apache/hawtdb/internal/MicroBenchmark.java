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
package org.apache.hawtdb.internal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javolution.io.Struct;

import org.apache.activemq.apollo.util.metric.MetricCounter;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.DataByteArrayOutputStream;
import org.apache.hawtdb.internal.Benchmarker.BenchmarkAction;
import org.junit.Test;

//import clojure.lang.IPersistentMap;
//import clojure.lang.PersistentHashMap;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class MicroBenchmark {
    
    // Are javolution.io.Struct objects faster than 
    // using DataOutputStream when writing to a direct byte buffer?
    
    interface BenchmarkMarshaller {
        BenchmarkMarshaller create();
        void marshall(ByteBuffer buffer);
    }
    
    public static final ThreadLocal<ByteBuffer> threadBuffer = new ThreadLocal<ByteBuffer>(); 
    
    public void marshalTest(final BenchmarkMarshaller type) throws Exception {
        final BenchmarkMarshaller marshaller = type.create();
        benchmark(5, new BenchmarkAction<MapActor>(type.getClass().getName()) {
            @Override
            protected void execute(MapActor actor) {

                ByteBuffer buffer = threadBuffer.get();
                if( buffer == null) {
                    buffer = ByteBuffer.allocateDirect(1024*4);
                }
                
                for (long i = 0; i < 100; i++) {
                    buffer.clear();
                    marshaller.marshall(buffer);
                }
            }
        });
    }
    
    
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

    static class JavolutionBenchmarkMarshaller implements BenchmarkMarshaller {
        @Override
        public void marshall(ByteBuffer buffer) {
            Header header = Header.create(buffer);
            header.magic.set("TEST");
            header.page.set(25);
            header.capacity.set(1024);
            header.size.set(40);
            header.active.set(10);
        }

        @Override
        public BenchmarkMarshaller create() {
            return new JavolutionBenchmarkMarshaller();
        }
    }
    
    @Test
    public void javolutionMarshallerTest() throws Exception {
        marshalTest(new JavolutionBenchmarkMarshaller());
    }
    
    
    static class StreamBenchmarkMarshaller implements BenchmarkMarshaller {
        static byte [] MAGIC = "TEST".getBytes();
        
        @Override
        public void marshall(ByteBuffer buffer) {
            DataByteArrayOutputStream os = new DataByteArrayOutputStream(20);
            try {
                os.write(MAGIC);
                os.writeInt(25);
                os.writeInt(1024);
                os.writeInt(40);
                os.writeInt(10);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            Buffer buffer2 = os.toBuffer();
            buffer.put(buffer2.data, buffer2.offset, buffer2.length);
        }

        @Override
        public BenchmarkMarshaller create() {
            return new StreamBenchmarkMarshaller();
        }
    }
    
    @Test
    public void streamMarshallerTest() throws Exception {
        marshalTest(new StreamBenchmarkMarshaller());
    }
    

// --------------------------------------------------------------------
// merging updates is on the critical path of the HawtTransaction 
// objects.  This was a little research on what what is the best way
// to do them.
// --------------------------------------------------------------------
    static interface Node {
        public Node create(Map<Long, Long> updates);
        public void merge(Node node);
        public Long get(Long i);
        public void clear();
    }
    
    public void nodeTest(final Node type) throws Exception {
        final Node node = type.create(new HashMap<Long, Long>());
        benchmark(5, new BenchmarkAction<MapActor>(type.getClass().getName()) {
            @Override
            protected void execute(MapActor actor) {
                
                // Test merges...
                for (long i = 0; i < 100; i++) {
                    Map<Long, Long> update = new HashMap<Long, Long>();
                    update.put(i, i);
                    node.merge(type.create(update));
                }
                
                // Do gets
                for (long i = 0; i < 100; i++) {
                    node.get(i);
                }
                
                node.clear();
            }
        });
    }
 
    static class SynchornizedMapNode implements Node {
        Map<Long, Long> updates;
        
        public SynchornizedMapNode() {
        }
        
        public SynchornizedMapNode(Map<Long, Long> updates) {
            this.updates = updates;
        }
        
        public Node create(Map<Long, Long> updates) {
            return new SynchornizedMapNode(updates);
        } 

        public void merge(Node n) {
            SynchornizedMapNode node = (SynchornizedMapNode) n;
            synchronized(node) {
                synchronized(this) {
                    node.updates.putAll(this.updates);
                    this.updates = node.updates;
                }
            }
        }

        synchronized public Long get(Long i) {
            return updates.get(i);
        }

        synchronized public void clear() {
            updates.clear();
        }

    }
        
    @Test
    public void SynchornizedMapNode() throws Exception {
        nodeTest(new SynchornizedMapNode());
    }
    
// --------------------------------------------------------------------
//   These were here to compare Java maps against the Clojure map
//   implementations.  Commented out to remove the dependency from 
//   the build.
// --------------------------------------------------------------------
//    
//    static class ClojureMapNode implements Node {
//        volatile IPersistentMap updates;
//        
//        public ClojureMapNode() {
//        }
//        
//        public ClojureMapNode(Map<Long, Long> updates) {
//            this.updates = PersistentHashMap.create(updates);
//        }
//        
//        public Node create(Map<Long, Long> updates) {
//            return new SynchornizedMapNode(updates);
//        } 
//
//        public void merge(Node n) {
//            ClojureMapNode node = (ClojureMapNode) n;
//            this.updates = (IPersistentMap) node.updates.cons(this.updates);
//        }
//
//        synchronized public Long get(Long i) {
//            return (Long) updates.valAt(i);
//        }
//
//        synchronized public void clear() {
//            updates = PersistentHashMap.EMPTY;
//        }
//
//    }
    
//    @Test
//    public void ClojureMapNode() throws Exception {
//        nodeTest(new ClojureMapNode());
//    }


    
    static class MapActor extends ActionActor<MapActor> {
    }

    private void benchmark(int count, BenchmarkAction<MapActor> action) throws Exception {
        Benchmarker benchmark = new Benchmarker();
        benchmark.setName(action.getName());
        ArrayList<MapActor> actors = createActors(count, action);
        benchmark.benchmark(actors, createMetrics(action));
    }

    protected ArrayList<MetricCounter> createMetrics(BenchmarkAction<MapActor> action) {
        ArrayList<MetricCounter> metrics = new ArrayList<MetricCounter>();
        metrics.add(action.success);
        metrics.add(action.failed);
        return metrics;
    }

    protected ArrayList<MapActor> createActors(int count, Action<MapActor> action) {
        ArrayList<MapActor> actors = new ArrayList<MapActor>();
        for (int i = 0; i < count; i++) {
            MapActor actor = new MapActor();
            actor.setName("actor:"+i);
            actor.setAction(action);
            actors.add(actor);
        }
        return actors;
    }
}
