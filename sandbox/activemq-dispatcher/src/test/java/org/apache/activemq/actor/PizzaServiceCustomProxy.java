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

package org.apache.activemq.actor;

import org.apache.activemq.dispatch.DispatchQueue;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class PizzaServiceCustomProxy implements IPizzaService {
    private final DispatchQueue queue;
    private final IPizzaService target;
    
    public PizzaServiceCustomProxy(IPizzaService target, DispatchQueue queue) {
        this.queue = queue;
        this.target=target;
    }

    public void order(final long count) {
        queue.dispatchAsync(new Runnable(){
            public void run() {
                target.order(count);
            }
        });
    }
    
}