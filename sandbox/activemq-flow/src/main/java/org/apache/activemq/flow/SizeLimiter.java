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

public class SizeLimiter<E> extends AbstractLimiter<E> implements IFlowSizeLimiter<E> {

    protected long capacity;
    protected long resumeThreshold;

    private long size;
    private boolean throttled;
    private long reserved;

    public SizeLimiter(long capacity, long resumeThreshold) {
        this.capacity = capacity;
        throttled = false;
        this.resumeThreshold = resumeThreshold;
    }

    public boolean add(E elem) {
        return add(1, getElementSize(elem));
    }

    public boolean add(int count, long size) {
        this.size += size;
        if (this.size >= capacity) {
            throttled = true;
        }
        return throttled;
    }

    public final void remove(E elem) {
        remove(1, getElementSize(elem));
    }

    public void reserve(E elem) {
        reserved += getElementSize(elem);
    }

    public void releaseReserved() {
        if (reserved > 0) {
            long res = reserved;
            reserved = 0;
            remove(1, res);
        }
    }

    public void remove(int count, long s) {
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

    public long getCapacity() {
        return capacity;
    }

    public long getResumeThreshold() {
        return resumeThreshold;
    }

    public long getSize() {
        return size;
    }

    public void setCapacity(long capacity) {
        if (capacity < resumeThreshold) {
            throw new IllegalArgumentException("capacity less than resume threshold");
        }

        this.capacity = capacity;

        if (this.size >= capacity) {
            throttled = true;
        }
    }

    public void setResumeThreshold(long size) {

        if (capacity < resumeThreshold) {
            throw new IllegalArgumentException("capacity less than resume threshold");
        }
        if (throttled && this.size <= resumeThreshold) {
            throttled = false;
            notifyUnThrottleListeners();
        }
    }

    public String toString() {
        return "{ capacity: " + capacity + ", resumeThreshold: " + resumeThreshold + ", size: " + size + ", reserved: " + reserved + ", throttled: " + throttled+" }";
    }
}
