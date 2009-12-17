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
package org.apache.activemq.actor;

import java.util.concurrent.TimeUnit;

import org.apache.activemq.dispatch.internal.AbstractSerialDispatchQueue;
import org.junit.Test;

import static java.lang.String.*;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class ActorBenchmark {

    public static class PizzaService implements IPizzaService {
        long counter;

        public void order(long count) {
            counter += count;
        }
    }

    @Test
    public void benchmarkCustomProxy() throws Exception {
        String name = "custom proxy";
        PizzaService service = new PizzaService();
        IPizzaService proxy = new PizzaServiceCustomProxy(service, createQueue());
        benchmark(name, service, proxy);
    }

    @Test
    public void benchmarkAsmProxy() throws Exception {
        String name = "asm proxy";
        PizzaService service = new PizzaService();
        IPizzaService proxy = ActorProxy.create(IPizzaService.class, service, createQueue());
        benchmark(name, service, proxy);
    }

    private AbstractSerialDispatchQueue createQueue() {
        return new AbstractSerialDispatchQueue("mock queue") {
            public void dispatchAsync(Runnable runnable) {
                runnable.run();
            }
            public void dispatchAfter(Runnable runnable, long delay, TimeUnit unit) {
            }
        };
    }

    private void benchmark(String name, PizzaService service, IPizzaService proxy) throws Exception {
        // warm it up..
        benchmark(proxy, 1000 * 1000);
        if (service.counter == 0)
            throw new Exception();

        int iterations = 1000 * 1000 * 100;

        long start = System.nanoTime();
        benchmark(proxy, iterations);
        long end = System.nanoTime();

        if (service.counter == 0)
            throw new Exception();

        double durationMS = 1.0d * (end - start) / 1000000d;
        double rate = 1000d * iterations / durationMS;
        System.out.println(format("name: %s, duration: %,.3f ms, rate: %,.2f executions/sec", name, durationMS, rate));
    }

    private void benchmark(IPizzaService proxy, int iterations) {
        for (int i = 0; i < iterations; i++) {
            proxy.order(1);
        }
    }

}
