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
package org.apache.activemq.flow;

import java.util.LinkedList;

public abstract class AbstractLimiter<E> implements IFlowLimiter<E> {

    private LinkedList<UnThrottleListener> throttleListeners = new LinkedList<UnThrottleListener>();
    private boolean resuming;

    public AbstractLimiter() {
    }

    public void addUnThrottleListener(UnThrottleListener l) {
        throttleListeners.add(l);

        if (!resuming && !getThrottled()) {
            notifyUnThrottleListeners();
        }
    }

    protected final void notifyUnThrottleListeners() {
        resuming = true;
        while (!getThrottled() && !throttleListeners.isEmpty()) {
            UnThrottleListener l = throttleListeners.remove();
            l.onUnthrottled();
        }
        resuming = false;
    }

}
