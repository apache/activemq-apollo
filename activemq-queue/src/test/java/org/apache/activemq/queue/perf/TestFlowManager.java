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
package org.apache.activemq.queue.perf;

import org.apache.activemq.flow.Flow;
import org.apache.activemq.flow.SizeLimiter;
import org.apache.activemq.queue.ExclusiveQueue;
import org.apache.activemq.queue.IFlowQueue;
import org.apache.activemq.queue.MultiFlowQueue;

public class TestFlowManager {
    static  <T> IFlowQueue<T> createQueue(String name, Flow flow, int capacity, int resumeThreshold) {
        return createFlowQueue(flow, name, capacity, resumeThreshold);
    }

    static public <T> IFlowQueue<T> createFlowQueue(Flow flow, String name, int capacity, int resumeThreshold) {
        IFlowQueue<T> queue;
        if (flow != null) {
            queue = new ExclusiveQueue<T>(flow, name, new SizeLimiter<T>(capacity, resumeThreshold));
        } else {
            queue = new MultiFlowQueue<T>(name, capacity, resumeThreshold);
        }
        return queue;
    }

    static public Flow createFlow(String name) {
        Flow rc = new Flow(name, false);
        return rc;
    }

}
