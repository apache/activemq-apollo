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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.metric.MetricCounter;
import org.apache.hawtdb.internal.Benchmarker.BenchmarkAction;

//import clojure.lang.IPersistentMap;
//import clojure.lang.PersistentHashMap;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class MapBenchmark {

    static interface Node {
        public Node create(Map<Long, Long> updates);
        public void merge(Node node);
        public Long get(Long i);
        public void clear();
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
    
//    @Test
//    public void SynchornizedMapNode() throws Exception {
//        nodeTest(new SynchornizedMapNode());
//    }
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
    
    
    
//    
//    @Test
//    public void clojureMapManyReaders() throws Exception {
//        IPersistentMap x = PersistentHashMap.EMPTY;
//        for (long i = 0; i < 1000; i++) {
//            x = x.assoc(i, i);
//        }
//        final IPersistentMap map = x;
//        benchmark(5, new BenchmarkAction<MapActor>("ManyReaders:"+PersistentHashMap.class.getName()) {
//            @Override
//            protected void execute(MapActor actor) throws Exception {
//                // Get them all..
//                for (long j = 0; j < 10; j++) {
//                    for (long i = 0; i < 1000; i++) {
//                        map.valAt(i);
//                    }
//                }
//                // Do a bunch of misses
//                for (long i = 0; i < 1000; i++) {
//                    map.valAt(-i);
//                }
//            }
//        });
//    }
//    
//    @Test
//    public void javaMap() throws Exception {
//        benchmark(1, new BenchmarkAction<MapActor>(HashMap.class.getName()) {
//            @Override
//            protected void execute(MapActor actor) {
//                Map<Long, Long> map = new HashMap<Long, Long>();
//                for (long i = 0; i < 1000; i++) {
//                    map.put(i, i);
//                }
//                // Get them all..
//                for (long j = 0; j < 10; j++) {
//                    for (long i = 0; i < 1000; i++) {
//                        map.get(i);
//                    }
//                }
//                // Do a bunch of misses
//                for (long i = 0; i < 1000; i++) {
//                    map.get(-i);
//                }
//                // remove 1/2
//                for (long i = 0; i < 1000; i++) {
//                    if ((i % 2) == 0) {
//                        map.remove(i);
//                    }
//                }
//                // Remove 1/2, miss 1/2 /w misses.
//                for (long i = 0; i < 1000; i++) {
//                    map.remove(i);
//                }
//            }
//        });
//    }
//
//    @Test
//    public void clojureMap() throws Exception {
//        benchmark(1, new BenchmarkAction<MapActor>(PersistentHashMap.class.getName()) {
//            @Override
//            protected void execute(MapActor actor) throws Exception {
//                IPersistentMap map = PersistentHashMap.EMPTY;
//                for (long i = 0; i < 1000; i++) {
//                    map = map.assoc(i, i);
//                }
//                // Get them all..
//                for (long j = 0; j < 10; j++) {
//                    for (long i = 0; i < 1000; i++) {
//                        map.valAt(i);
//                    }
//                }
//                // Do a bunch of misses
//                for (long i = 0; i < 1000; i++) {
//                    map.valAt(-i);
//                }
//                // remove 1/2
//                for (long i = 0; i < 1000; i++) {
//                    if ((i % 2) == 0) {
//                        map = map.without(i);
//                    }
//                }
//                // Remove 1/2, miss 1/2 /w misses.
//                for (long i = 0; i < 1000; i++) {
//                    map = map.without(i);
//                }
//            }
//        });
//    }
//    
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
