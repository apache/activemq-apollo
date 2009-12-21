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

package org.apache.activemq.queue.actor.perf;

import org.apache.activemq.dispatch.DispatchObject;
import org.apache.activemq.dispatch.DispatchQueue;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class DispatchObjectFilter implements DispatchObject {
    protected DispatchObject next;
    
    public DispatchObjectFilter() {
    }

    public DispatchObjectFilter(DispatchObject next) {
        this.next = next;
    }
    
    public void addShutdownWatcher(Runnable shutdownWatcher) {
        next.addShutdownWatcher(shutdownWatcher);
    }
    public <Context> Context getContext() {
        return (Context)next.getContext();
    }
    public DispatchQueue getTargetQueue() {
        return next.getTargetQueue();
    }
    public void release() {
        next.release();
    }
    public void resume() {
        next.resume();
    }
    public void retain() {
        next.retain();
    }
    public <Context> void setContext(Context context) {
        next.setContext(context);
    }
    public void setTargetQueue(DispatchQueue queue) {
        next.setTargetQueue(queue);
    }
    public void suspend() {
        next.suspend();
    }
}