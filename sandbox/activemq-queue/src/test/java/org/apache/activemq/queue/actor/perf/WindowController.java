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

package org.apache.activemq.queue.actor.perf;

import org.apache.activemq.util.IntrospectionSupport;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class WindowController extends WindowLimiter {

    private int maxSize;
    private int processed;
    private int creditsAt;
    
    public int processed(int count) {
        int rc = 0;
        processed += count;
        if( processed >= creditsAt ) {
            change(processed);
            rc = processed;
            processed = 0;
        }
        return rc;
    }
    
    int maxSize(int newMaxSize) {
        int change = newMaxSize-maxSize;
        this.maxSize=newMaxSize;
        this.creditsAt = maxSize/2;
        change(change);
        return change;
    }
    
    int maxSize() {
        return maxSize;
    }
    
    @Override
    public String toString() {
        return IntrospectionSupport.toString(this);
    }

}