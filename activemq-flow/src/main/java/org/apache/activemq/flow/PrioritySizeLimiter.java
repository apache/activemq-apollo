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

import java.util.ArrayList;

import org.apache.activemq.util.Mapper;

public class PrioritySizeLimiter<E> {

    final private ArrayList<Priority> priorities = new ArrayList<Priority>();
    final protected int capacity;
    final protected int resumeThreshold;

    private int totalSize;
    private int throttledCount = 0;
    private int highestPriority;

    private Mapper<Integer, E> sizeMapper = new Mapper<Integer, E>() {
        public Integer map(E element) {
            return 1;
        }
    };

    private Mapper<Integer, E> priorityMapper = sizeMapper;

    private class Priority extends AbstractLimiter<E> implements IFlowSizeLimiter<E> {
        final int priority;
        long size;
        int reserved;
        private boolean throttled;

        public Priority(int priority) {
            this.priority = priority;
        }

        public long getSize() {
            return size;
        }

        public long getCapacity() {
            return capacity;
        }

        public boolean add(int count, long elementSize) {
            totalSize += elementSize;
            size += elementSize;
            if (totalSize >= capacity) {
                if (!throttled) {
                    throttled = true;
                    throttledCount++;
                }
            }
            if (priority >= highestPriority) {
                highestPriority = priority;
            }
            return throttled;
        }

        public boolean add(E elem) {
            return add(1, sizeMapper.map(elem));
        }

        public boolean canAdd(E elem) {
            if (throttled)
                return false;

            int prio = priorityMapper.map(elem);
            if (prio < highestPriority) {
                if (!throttled) {
                    throttled = true;
                    throttledCount++;
                }
                return false;
            }

            return true;
        }

        public boolean getThrottled() {
            return throttled;
        }

        public void releaseReserved() {
            if (reserved > 0) {
                int res = reserved;
                reserved = 0;
                remove(1, res);
            }
        }

        public void remove(E elem) {
            int size = sizeMapper.map(elem);
            remove(1, size);
        }

        public void remove(int c, long s) {
            size -= s;
            totalSize -= s;

            assert size >= 0 : "Negative limiter size: " + size;
            assert totalSize >= 0 : "Negative limiter total size:" + totalSize;

            if (totalSize <= resumeThreshold) {
                priorities.get(highestPriority).unThrottle();
            }
        }

        public void unThrottle() {
            if (throttled) {
                throttled = false;
                throttledCount--;
                notifyUnThrottleListeners();

                // Has the highest priority level emptied out?
                if (size == 0 && priority == highestPriority) {
                    // Set highestPriority to the new highest priority level
                    highestPriority = 0;
                    for (int i = priority - 1; i >= 0; i--) {
                        Priority p = priorities.get(i);
                        if (p.size > 0 || p.throttled) {
                            highestPriority = i;
                            if (totalSize <= resumeThreshold) {
                                p.unThrottle();
                            }
                        }
                    }
                }
            }
        }

        public void reserve(E elem) {
            reserved += sizeMapper.map(elem);
        }

        public int getElementSize(E elem) {
            return sizeMapper.map(elem);
        }
    }

    public long getCapacity() {
        return capacity;
    }
    
    public long getSize() {
        return totalSize;
    }

    public PrioritySizeLimiter(int capacity, int resumeThreshold, int priorities) {
        this.capacity = capacity;
        this.resumeThreshold = resumeThreshold;
        for (int i = 0; i < priorities; i++) {
            this.priorities.add(new Priority(i));
        }
    }

    public IFlowSizeLimiter<E> getPriorityLimter(int priority) {
        return priorities.get(priority);
    }

    public Mapper<Integer, E> getSizeMapper() {
        return sizeMapper;
    }

    public void setSizeMapper(Mapper<Integer, E> sizeMapper) {
        this.sizeMapper = sizeMapper;
    }

    public Mapper<Integer, E> getPriorityMapper() {
        return priorityMapper;
    }

    public void setPriorityMapper(Mapper<Integer, E> priorityMapper) {
        this.priorityMapper = priorityMapper;
    }

    public boolean getThrottled() {
        return throttledCount > 0;
    }

    public int getPriorities() {
        return priorities.size();
    }

}