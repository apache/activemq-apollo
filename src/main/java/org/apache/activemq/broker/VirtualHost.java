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
package org.apache.activemq.broker;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.activemq.Service;
import org.apache.activemq.protobuf.AsciiBuffer;

/**
 * @author chirino
 */
public class VirtualHost implements Service {
    
    private ArrayList<AsciiBuffer> hostNames = new ArrayList<AsciiBuffer>();
    private Router router = new Router();
    private HashMap<Destination, Queue> queues = new HashMap<Destination, Queue>();

    public AsciiBuffer getHostName() {
        if( hostNames.size() > 0 ) {
            hostNames.get(0);
        }
        return null;
    }
    
    public ArrayList<AsciiBuffer> getHostNames() {
        return hostNames;
    }
    public void setHostNames(ArrayList<AsciiBuffer> hostNames) {
        this.hostNames = hostNames;
    }
    
    public Router getRouter() {
        return router;
    }
    
    public void start() throws Exception {
        for (Queue queue : queues.values()) {
            queue.start();
        }
    }
    public void stop() throws Exception {
        for (Queue queue : queues.values()) {
            queue.stop();
        }
    }

    public void addQueue(Queue queue) {
        Domain domain = router.getDomain(queue.getDestination().getDomain());
        domain.add(queue.getDestination().getName(), queue);
    }


}
