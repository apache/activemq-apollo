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
package org.apache.activemq.apollo.store;

/**
 * Result Holder for queue related queries.
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class QueueStatus {

    /**
     * The descriptor for the queue.
     */
    public QueueRecord record;

    /**
     * Gets the count of elements in this queue. Note that this does not
     * include counts for elements held in child partitions.
     */
    public int count;

    /**
     * The size of elements in this queue. Note that this does not
     * include size of elements held in child partitions.
     */
    public long size;

    /**
     * The first sequence number in the queue.
     */
    public long first;

    /**
     * The last sequence number in the queue.
     */
    public long last;

}
