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

import java.util.concurrent.Executor;

final class PooledPriorityDispatcher extends AbstractPooledDispatcher {
    private final int numPriorities;

    PooledPriorityDispatcher(String name, int size, int numPriorities) {
        super(name, size);
        this.numPriorities = numPriorities;
    }

    @Override
    protected final PriorityDispatcher createDispatcher(String name, AbstractPooledDispatcher pool) throws Exception {
        // TODO Auto-generated method stub
        return new PriorityDispatcher(name, numPriorities, this);
    }

    public PriorityDispatcher chooseDispatcher() {
        return ((PriorityDispatcher)super.chooseDispatcher());
    }
    
    public final Executor createPriorityExecutor(final int priority) {
        return new Executor() {
            public void execute(final Runnable runnable) {
                chooseDispatcher().dispatch(new RunnableAdapter(runnable), priority);
            }

        };
    }

    public int getDispatchPriorities() {
        // TODO Auto-generated method stub
        return numPriorities;
    }

    public void execute(Runnable command) {
        chooseDispatcher().dispatch(new RunnableAdapter(command), 0);
    }

}