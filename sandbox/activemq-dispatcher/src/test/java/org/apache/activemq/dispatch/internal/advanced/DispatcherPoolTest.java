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
package org.apache.activemq.dispatch.internal.advanced;

import java.util.concurrent.CountDownLatch;

import org.apache.activemq.dispatch.DispatchQueue;
import org.apache.activemq.dispatch.DispatcherConfig;

import static java.lang.String.*;
import static org.apache.activemq.dispatch.DispatchOption.*;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class DispatcherPoolTest {
    
    public static void main(String[] args) throws Exception {
        AdvancedDispatcher dispatcher = new AdvancedDispatcher(new DispatcherConfig());
        dispatcher.resume();
        
        // warm the JIT up..
        benchmarkWork(dispatcher, 100000);
        
        int iterations = 1000*1000*20;
        long start = System.nanoTime();
        benchmarkWork(dispatcher, iterations);
        long end = System.nanoTime();
        
        double durationMS = 1.0d*(end-start)/1000000d;
        double rate = 1000d * iterations / durationMS;
        
        dispatcher.release();
        System.out.println(format("duration: %,.3f ms, rate: %,.2f executions/sec", durationMS, rate));
    }

    private static void benchmarkWork(final AdvancedDispatcher pooledDispatcher, int iterations) throws InterruptedException {
        final CountDownLatch counter = new CountDownLatch(iterations);
        for (int i = 0; i < 1000; i++) {
            Work dispatchable = new Work(counter, pooledDispatcher);
            dispatchable.dispatchQueue.dispatchAsync(dispatchable);
        }
        counter.await();
    }
    
    private static final class Work implements Runnable {
        private final CountDownLatch counter;
        private final DispatchQueue dispatchQueue;

        private Work(CountDownLatch counter, AdvancedDispatcher dispatcher) {
            this.counter = counter;
            dispatchQueue = dispatcher.createSerialQueue("test", STICK_TO_CALLER_THREAD);
        }

        public void run() {
            counter.countDown();
            if( counter.getCount()>0 ) {
                dispatchQueue.dispatchAsync(this);
            }
        }
    }

    
}
