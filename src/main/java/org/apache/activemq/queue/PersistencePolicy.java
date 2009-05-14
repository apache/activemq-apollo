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
package org.apache.activemq.queue;

public interface PersistencePolicy<E> {

    /**
     * Indicates whether the given element should be persisted on enqueue.
     * 
     * @param elem
     *            The enqueued element.
     * @return True if the element must be persisted on enqueue.
     */
    public boolean isPersistent(E elem);

    /**
     * Indicated whether or not paging is enabled for this queue. When not
     * enabled elements are kept in memory until they are removed. When enabled,
     * elements are paged to persistent storage when
     * {@link #getPagingInMemorySize()} is exceeded();
     * 
     * @return Whether or not paging is enabled for the queue.
     */
    public boolean isPagingEnabled();

    /**
     * When paging is enabled this specifies whether the queue can keep a place
     * holder for paged out elements in memory. Keeping place holders in memory
     * improves performance, but for very large queues keeping place holders can
     * cause a significant overhead.
     * 
     * This method must return false if {@link #isPagingEnabled()} is false.
     * 
     * @return True if the queue should page out place holders.
     */
    public boolean isPageOutPlaceHolders();

    /**
     * Sets the memory threshold after which elements are paged out to disk to
     * conserve memory.
     * 
     * @return The In Memory Paging Threshold.
     */
    public int getPagingInMemorySize();

    /**
     * Indicates whether sources should be throttled to the memory limit
     * while the queue is being actively consumed. 
     *  
     * @return true if sources should be throttled to the memory limit while
     * the queue is being consumed. 
     */
    public boolean isThrottleSourcesToMemoryLimit();
    
    /**
     * When the queue is being restored this indicates the preference to
     * allow for recovering elements from the store versus newly added
     * elements. For example if set to 3 then the queue will try to catch up
     * by 3 elements for each newly added one. 
     */
    public int getRecoveryBias();
    
    /**
     * The rate at which to throttle sources when the queue is stopped or
     * or is not actively being consumed. 
     * 
     * @return the rate at which to throttle source when the queue is not
     * being consumed. A value less than or equal to 0 means no limit. 
     */
    public int getDisconnectedThrottleRate();
    

    public static final class NON_PERSISTENT_POLICY<E> implements PersistencePolicy<E> {

        /*
         * (non-Javadoc)
         * 
         * @see
         * org.apache.activemq.queue.PersistencePolicy#isPersistOnEnqueue(java
         * .lang.Object)
         */
        public boolean isPersistent(E elem) {
            return false;
        }

        /*
         * (non-Javadoc)
         * 
         * @see
         * org.apache.activemq.queue.PersistencePolicy#isKeepElementRefences()
         */
        public boolean isPageOutPlaceHolders() {
            return false;
        }

        /*
         * (non-Javadoc)
         * 
         * @see org.apache.activemq.queue.PersistencePolicy#isPagingEnabled()
         */
        public boolean isPagingEnabled() {
            return false;
        }

        /*
         * (non-Javadoc)
         * 
         * @see
         * org.apache.activemq.queue.PersistencePolicy#getPagingInMemoryThreshold
         * ()
         */
        public int getPagingInMemorySize() {
            // TODO Auto-generated method stub
            return Integer.MAX_VALUE;
        }

        /* (non-Javadoc)
         * @see org.apache.activemq.queue.PersistencePolicy#getDisconnectedThrottleRate()
         */
        public int getDisconnectedThrottleRate() {
            // TODO Auto-generated method stub
            return 0;
        }

        /* (non-Javadoc)
         * @see org.apache.activemq.queue.PersistencePolicy#isThrottleSourcesToMemoryLimit()
         */
        public boolean isThrottleSourcesToMemoryLimit() {
            return true;
        }

        /* (non-Javadoc)
         * @see org.apache.activemq.queue.PersistencePolicy#getRecoveryBias()
         */
        public int getRecoveryBias() {
            return Integer.MAX_VALUE;
        }

    }

}
