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

import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.util.IntrospectionSupport;

/**
 */
final public class Flow {
    static final private AtomicLong FLOW_COUNTER = new AtomicLong();

    final private long flowID;
    final private String flowName;
    final private boolean dynamic;
    final private int hashCode;

    /**
     * Package scoped constructor.
     * 
     * @param name
     *            The flow name.
     * @param id
     *            The flow id.
     */
    public Flow(String name, boolean dynamic) {
        this.flowID = FLOW_COUNTER.incrementAndGet();
        this.flowName = name;
        this.dynamic = dynamic;
        this.hashCode = (int) (flowID ^ (flowID >>> 32));
    }

    /**
     * @see org.apache.activemq.flow.Flow#getFlowID()
     */
    public long getFlowID() {
        return flowID;
    }

    /**
     * @see Flow#getFlowName()
     */
    public String getFlowName() {
        return flowName;
    }

    /**
     * @see Flow#isDynamic()
     */
    public boolean isDynamic() {
        return dynamic;
    }

    public int hashCode() {
        return hashCode;
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o instanceof Flow) {
            return equals((Flow) o);
        }

        return false;
    }

    public boolean equals(Flow flow) {
        return flowID == flow.getFlowID();
    }

    public String toString() {
        return IntrospectionSupport.toString(this);
    }
}
