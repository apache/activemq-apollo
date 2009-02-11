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

public class SizeLimiter<E> extends AbstractLimiter<E> {

    protected int capacity;
    protected int resumeThreshold;

    private int size;
    private boolean throttled;
    private int reserved;

    public SizeLimiter(int capacity, int resumeThreshold) {
        this.capacity = capacity;
        throttled = false;
        this.resumeThreshold = resumeThreshold;
    }

    public final boolean add(E elem) {
        this.size += getElementSize(elem);

        if (this.size >= capacity) {
            throttled = true;
        }
        return throttled;
    }

    public final void remove(E elem) {
        remove(getElementSize(elem));
    }

    public void reserve(E elem) {
        reserved += getElementSize(elem);
    }

    public void releaseReserved() {
        if (reserved > 0) {
            int res = reserved;
            reserved = 0;
            remove(res);
        }
    }

    protected void remove(int s) {
        this.size -= s;
        if (size < 0) {
            Exception ie = new IllegalStateException("Size Negative!" + size);
            ie.printStackTrace();
        }

        if (throttled && this.size <= resumeThreshold) {
            throttled = false;
            notifyUnThrottleListeners();
        }
    }

    public final void reset() {
        size = 0;
        notifyUnThrottleListeners();
    }

    /**
     * Subclasses should override to return the size of an element
     * 
     * @param elem
     */
    public int getElementSize(E elem) {
        return 1;
    }

    public final boolean getThrottled() {
        return throttled;
    }

    public boolean canAdd(E elem) {
        return !throttled;
    }

    public int getCapacity() {
        return capacity;
    }

    public int getResumeThreshold() {
        return resumeThreshold;
    }

    public int getSize() {
        return size;
    }
}
