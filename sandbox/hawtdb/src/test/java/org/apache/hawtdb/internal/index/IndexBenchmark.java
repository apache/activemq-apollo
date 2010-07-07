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

import java.io.File;
import java.util.Random;

import org.fusesource.hawtbuf.Buffer;
import org.apache.hawtdb.api.Index;
import org.apache.hawtdb.api.OutOfSpaceException;
import org.apache.hawtdb.api.Transaction;
import org.apache.hawtdb.internal.Action;
import org.apache.hawtdb.internal.Benchmarker.BenchmarkAction;
import org.apache.hawtdb.internal.page.HawtPageFile;
import org.apache.hawtdb.internal.page.HawtPageFileFactory;
import org.apache.hawtdb.internal.page.TransactionActor;
import org.apache.hawtdb.internal.page.TransactionBenchmarker;
import org.junit.Test;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public abstract class IndexBenchmark {
    
    private static final int KEY_SPACE = 5000000;
    private static final int VALUE_SIZE = 8;
    
    static final public byte[] DATA = new byte[VALUE_SIZE];

    class IndexActor extends TransactionActor<IndexActor> {
        public Random random;
        public Index<Long, Buffer> index;
        long counter=0;
        
        public void setName(String name) {
            super.setName(name);
            this.random = new Random(name.hashCode());
        }
        
        @Override
        public void setTx(Transaction tx) {
            super.setTx(tx);
            index = createIndex(tx);
        }
        
        public void benchmarkIndex() throws InterruptedException {
            // Transaction retry loop.
            while( true ) {
                try {
                    
                    index.put(counter++, new Buffer(DATA));
                    if( (counter%KEY_SPACE)==0 ) {
                        counter=0;
                    }
                    
                    // Transaction succeeded.. break out of retry loop. 
                    break;
                    
                } catch (OutOfSpaceException e) {
                    counter--;
                    tx().rollback();
                    System.out.println("OutOfSpaceException occurred.. waiting for space to free up..");
                    Thread.sleep(500);
                } finally {
                    tx().commit();
                }
            }
        }
    }
    
    TransactionBenchmarker<IndexActor> benchmark = new TransactionBenchmarker<IndexActor>() {
        protected IndexActor createActor(HawtPageFile pageFile, Action<IndexActor> action, int i) {
            return new IndexActor();
        };
    };

    public IndexBenchmark() {
        HawtPageFileFactory hawtPageFileFactory = new HawtPageFileFactory();
        hawtPageFileFactory.setFile(new File("target/test-data/" + getClass().getName() + ".db"));
        hawtPageFileFactory.setSync(false);
         // Limit file growth to 1 Gig.
        hawtPageFileFactory.setMaxFileSize(1024*1024*1024); 
        benchmark.setHawtPageFileFactory(hawtPageFileFactory);
    }
    
    @Test
    public void insert() throws Exception {
        benchmark.benchmark(10, new BenchmarkAction<IndexActor>("insert") {
            protected void execute(IndexActor actor) throws InterruptedException {
                actor.benchmarkIndex();
            }
        });
    }


    abstract protected Index<Long, Buffer> createIndex(Transaction tx);

}
