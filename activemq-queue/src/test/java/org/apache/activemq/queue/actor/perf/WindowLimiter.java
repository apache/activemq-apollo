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

import org.apache.activemq.util.IntrospectionSupport;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class WindowLimiter {

    private int opensAt = 1;
    private int size;
    private boolean closed;
    
    public WindowLimiter() {
        this.closed = true;
    }
    
    int opensAt() {
        return opensAt;
    }
    
    WindowLimiter opensAt(int opensAt) {
        this.opensAt = opensAt;
        closed = size < opensAt ;
        return this;
    }

    int size() {
        return size;
    }
    
    WindowLimiter size(int size) {
        this.size = size;
        closed = size < opensAt ;
        return this;
    }
    
    public boolean isOpen() {
        return !closed;
    }
    
    public boolean isClosed() {
        return closed;
    }
    
    public void change(int change) {
        size += change;
        if( change > 0 && closed && size >= opensAt) {
            closed = false;
        } else if( change < 0 && !closed && size <= 0) {
            closed = true;
        }
    }

    @Override
    public String toString() {
        return IntrospectionSupport.toString(this);
    }

}