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

import org.apache.activemq.util.buffer.AsciiBuffer;

public class QueueDescriptor {

    public static final short SHARED = 0;
    public static final short SHARED_PRIORITY = 1;
    public static final short PARTITIONED = 2;
    public static final short EXCLUSIVE = 4;
    public static final short EXCLUSIVE_PRIORITY = 5;
    
    AsciiBuffer queueName;
    AsciiBuffer parent;
    int partitionKey;
    short applicationType;
    short queueType = SHARED;

    public QueueDescriptor() {
    }

    public QueueDescriptor(QueueDescriptor toCopy) {
        if (toCopy == null) {
            return;
        }
        queueName = toCopy.queueName;
        applicationType = toCopy.applicationType;
        queueType = toCopy.queueType;
        partitionKey = toCopy.partitionKey;
        parent = toCopy.parent;
    }

    public QueueDescriptor copy() {
        return new QueueDescriptor(this);
    }

    public int getPartitionKey() {
        return partitionKey;
    }

    public void setPartitionId(int key) {
        this.partitionKey = key;
    }

    /**
     * Sets the queue type which is useful for querying of queues. The value
     * must not be less than 0.
     * 
     * @param type
     *            The type of the queue.
     */
    public void setApplicationType(short type) {
        if (type < 0) {
            throw new IllegalArgumentException();
        }
        applicationType = type;
    }

    /**
     * @param type
     *            The type of the queue.
     */
    public short getApplicationType() {
        return applicationType;
    }

    public short getQueueType() {
        return queueType;
    }

    public void setQueueType(short type) {
        queueType = type;
    }

    /**
     * If this queue is a partition of a parent queue, this should be set to
     * the parent queue's name.
     * 
     * @return The parent queue's name
     */
    public AsciiBuffer getParent() {
        return parent;
    }

    /**
     * If this queue is a partition of a parent queue, this should be set to
     * the parent queue's name.
     */
    public void setParent(AsciiBuffer parent) {
        this.parent = parent;
    }

    public AsciiBuffer getQueueName() {
        return queueName;
    }

    public void setQueueName(AsciiBuffer queueName) {
        this.queueName = queueName;
    }

    public int hashCode() {
        return queueName.hashCode();
    }

    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (o == this) {
            return true;
        }

        if (o instanceof QueueDescriptor) {
            return equals((QueueDescriptor) o);
        } else {
            return false;
        }
    }

    public boolean equals(QueueDescriptor qd) {
        if (qd.queueName.equals(queueName)) {
            return true;
        }
        return false;
    }
}