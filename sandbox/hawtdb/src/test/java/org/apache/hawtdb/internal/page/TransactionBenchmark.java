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

import java.util.Random;

import org.fusesource.hawtbuf.Buffer;
import org.apache.hawtdb.api.Transaction;
import org.apache.hawtdb.internal.Action;
import org.apache.hawtdb.internal.Benchmarker.BenchmarkAction;
import org.apache.hawtdb.internal.page.HawtPageFile;
import org.apache.hawtdb.internal.page.HawtPageFileFactory;
import org.apache.hawtdb.internal.page.TransactionBenchmarker.Callback;
import org.junit.Test;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class TransactionBenchmark {

    static private byte[] THE_DATA = new byte[1024 * 3];

    static class RandomTxActor extends TransactionActor<RandomTxActor> {
        public Random random;
        public void setName(String name) {
            super.setName(name);
            this.random = new Random(name.hashCode());
        }
    }
    
    TransactionBenchmarker<RandomTxActor> benchmark = new TransactionBenchmarker<RandomTxActor>() {
        protected RandomTxActor createActor(HawtPageFile pageFile, Action<RandomTxActor> action, int i) {
            return new RandomTxActor();
        };
    };
    
//    @Test
//    public void append() throws Exception {
//        benchmark.benchmark(1, new BenchmarkAction<RandomTxActor>("append") {
//            @Override
//            protected void execute(RandomTxActor actor) {
//                int page = actor.tx().allocator().alloc(1);
//                actor.tx().write(page, new Buffer(THE_DATA));
//                actor.tx().commit();
//            }
//        });
//    }

    @Test
    public void aupdate() throws Exception {
        final int INITIAL_PAGE_COUNT = 1024 * 100;
        preallocate(INITIAL_PAGE_COUNT);
        benchmark.benchmark(1, new BenchmarkAction<RandomTxActor>("update") {
            @Override
            protected void execute(RandomTxActor actor) {
                int page = actor.random.nextInt(INITIAL_PAGE_COUNT);
                actor.tx().write(page, new Buffer(THE_DATA));
                actor.tx().commit();
            }
        });
    }

    
    @Test
    public void read() throws Exception {
        final int INITIAL_PAGE_COUNT = 1024 * 100;
        preallocate(INITIAL_PAGE_COUNT);
        benchmark.benchmark(1, new BenchmarkAction<RandomTxActor>("read") {
            @Override
            protected void execute(RandomTxActor actor) {
                int page = actor.random.nextInt(INITIAL_PAGE_COUNT);
                actor.tx().read(page, new Buffer(THE_DATA));
                actor.tx().commit();
            }
        });
    }
    
    
    private void preallocate(final int INITIAL_PAGE_COUNT) {
        benchmark.setSetup(new Callback(){
            public void run(HawtPageFileFactory pff) throws Exception {
                Transaction tx = pff.getConcurrentPageFile().tx();
                for (int i = 0; i < INITIAL_PAGE_COUNT; i++) {
                    int page = tx.allocator().alloc(1);
                    tx.write(page, new Buffer(THE_DATA));
                }
                tx.commit();
            }
        });
    }

}
