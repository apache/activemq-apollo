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

import java.util.concurrent.TimeUnit;

final class TimerEvent extends ForeignEvent {

    private final long delay;
    private final Runnable wrapper;
    private final TimeUnit timeUnit;
    private final DispatcherThread thread;

    public TimerEvent(DispatcherThread thread, Runnable wrapper, long delay, TimeUnit timeUnit) {
        this.thread = thread;
        this.delay = delay;
        this.wrapper = wrapper;
        this.timeUnit = timeUnit;
    }

    public void run() {
        thread.timerHeap.addRelative(wrapper, delay, timeUnit);
    }
}