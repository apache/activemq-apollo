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

package org.apache.activemq.syscall.util;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.*;

public class RateSampler extends Thread {
    private final AtomicReference<ArrayList<Double>> samples = new AtomicReference<ArrayList<Double>>();
    private final AtomicLong metric;
    private final int count;
    private final long period;
    public boolean verbose;
    
    public RateSampler(AtomicLong metric, int count, double periodInSecs) {
        super("Sampler");
        this.metric = metric;
        this.count = count;
        this.period = ns(periodInSecs);
        setPriority(MAX_PRIORITY);
        setDaemon(true);
    }
    
    static final long NANOS_PER_SECOND = NANOSECONDS.convert(1, SECONDS);

    static private long ns(double v) {
        return (long)(v*NANOS_PER_SECOND);
    }

    
    @Override
    public void run() {
        ArrayList<Double> samples = new ArrayList<Double>(count);
        try {
            long sleepMS = period/1000000;
            int sleepNS = (int) (period%1000000);
            long currentValue, now;
            long lastValue = metric.get();
            long lastTime = System.nanoTime();
            

            for (int i = 0; i < count; i++) {
                if( verbose ) {
                    System.out.print(".");
                }
                Thread.sleep(sleepMS,sleepNS);
                
                now = System.nanoTime();
                currentValue = metric.get();
                
                double t = (now-lastTime);
                t = t/NANOS_PER_SECOND;
                t = (currentValue-lastValue)/t;
                samples.add(t);
                
                lastTime=now;
                lastValue=currentValue;
            }
        } catch (InterruptedException e) {
        } finally {
            this.samples.set(samples);
        }
    }
    
    public synchronized ArrayList<Double> getSamples() {
        return samples.get();
    }
}