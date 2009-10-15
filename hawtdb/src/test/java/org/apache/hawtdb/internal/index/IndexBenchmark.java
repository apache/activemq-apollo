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

import java.util.Random;

import org.apache.activemq.util.buffer.Buffer;
import org.apache.hawtdb.api.Index;
import org.apache.hawtdb.api.Transaction;
import org.apache.hawtdb.internal.Action;
import org.apache.hawtdb.internal.Benchmarker.BenchmarkAction;
import org.apache.hawtdb.internal.page.ConcurrentPageFile;
import org.apache.hawtdb.internal.page.TransactionActor;
import org.apache.hawtdb.internal.page.TransactionBenchmarker;
import org.junit.Test;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public abstract class IndexBenchmark {
    
    static final public byte[] DATA = new byte[8];

    class IndexActor extends TransactionActor<IndexActor> {
        public Random random;
        public Index<Long, Buffer> index;
        
        public void setName(String name) {
            super.setName(name);
            this.random = new Random(name.hashCode());
        }
        
        @Override
        public void setTx(Transaction tx) {
            super.setTx(tx);
            index = createIndex(tx);
        }
    }
    
    TransactionBenchmarker<IndexActor> benchmark = new TransactionBenchmarker<IndexActor>() {
        protected IndexActor createActor(ConcurrentPageFile pageFile, Action<IndexActor> action, int i) {
            return new IndexActor();
        };
    };

    
    @Test
    public void insert() throws Exception {
        benchmark.benchmark(1, new BenchmarkAction<IndexActor>("insert") {
            long counter=0;
            @Override
            protected void execute(IndexActor actor) {
                actor.index.put(counter++, new Buffer(DATA));
            }
        });
    }


    abstract protected Index<Long, Buffer> createIndex(Transaction tx);

}
