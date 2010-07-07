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

import java.io.File;
import java.util.ArrayList;

import org.apache.activemq.apollo.util.metric.MetricCounter;
import org.apache.hawtdb.internal.Action;
import org.apache.hawtdb.internal.Benchmarker;
import org.apache.hawtdb.internal.Benchmarker.BenchmarkAction;
import org.apache.hawtdb.internal.page.HawtPageFile;
import org.apache.hawtdb.internal.page.HawtPageFileFactory;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class TransactionBenchmarker<A extends TransactionActor<A>> {
    
    
    public interface Callback {
        public void run(HawtPageFileFactory pff) throws Exception;
    }
    
    private Callback setup;
    private Callback tearDown;
    private int samples = 3;
    private int period = 1000*5;
    private HawtPageFileFactory hawtPageFileFactory;
    
    public void benchmark(int actorCount, BenchmarkAction<A> action) throws Exception {
        HawtPageFileFactory pff = getHawtPageFileFactory();
        pff.getFile().delete();
        pff.open();
        try {
            if( setup!=null ) {
                setup.run(pff);
            }
            HawtPageFile pf = pff.getConcurrentPageFile();
            Benchmarker benchmark = new Benchmarker();
            benchmark.setSamples(samples);
            benchmark.setPeriod(period);
            benchmark.setName(action.getName());
            ArrayList<A> actors = createActors(pf, actorCount, action);
            benchmark.benchmark(actors, createMetrics(action));
        } finally {
            try {
                if( tearDown!=null ) {
                    tearDown.run(pff);
                }
            } finally {
                pff.close();
            }
        }
    }

    protected ArrayList<MetricCounter> createMetrics(BenchmarkAction<A> action) {
        ArrayList<MetricCounter> metrics = new ArrayList<MetricCounter>();
        metrics.add(action.success);
        metrics.add(action.failed);
        return metrics;
    }

    protected ArrayList<A> createActors(HawtPageFile pageFile, int count, Action<A> action) {
        ArrayList<A> actors = new ArrayList<A>();
        for (int i = 0; i < count; i++) {
            A actor = createActor(pageFile, action, i);
            actor.setName("actor:"+i);
            actor.setAction(action);
            actor.setTx(pageFile.tx());
            actors.add(actor);
        }
        return actors;
    }

    @SuppressWarnings("unchecked")
    protected A createActor(HawtPageFile pageFile, Action<A> action, int i) {
        return (A) new TransactionActor();
    }

    public Callback getSetup() {
        return setup;
    }

    public void setSetup(Callback setup) {
        this.setup = setup;
    }

    public Callback getTearDown() {
        return tearDown;
    }

    public void setTearDown(Callback tearDown) {
        this.tearDown = tearDown;
    }

    public int getSamples() {
        return samples;
    }

    public void setSamples(int samples) {
        this.samples = samples;
    }

    public int getPeriod() {
        return period;
    }

    public void setPeriod(int period) {
        this.period = period;
    }

    public void setHawtPageFileFactory(HawtPageFileFactory hawtPageFileFactory) {
        this.hawtPageFileFactory = hawtPageFileFactory;
    }
    public HawtPageFileFactory getHawtPageFileFactory() {
        if( hawtPageFileFactory==null ) {
            hawtPageFileFactory = new HawtPageFileFactory();
            hawtPageFileFactory.setFile(new File("target/test-data/" + getClass().getName() + ".db"));
        }
        return hawtPageFileFactory;
    }    
    
}
