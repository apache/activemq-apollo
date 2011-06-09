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
package org.apache.activemq.apollo.util;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * Holds a singleton instance to a cached thread pool that can be used
 * to execute blocking tasks.  The tasks must be independent of each other
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class ApolloThreadPool {

    private static long stackSize = Long.parseLong(System.getProperty("apollo.thread.stack.size", ""+1024*512));

    public static final ThreadPoolExecutor INSTANCE = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 10, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(), new ThreadFactory() {
        public Thread newThread(Runnable r) {
            Thread rc = new Thread(null, r, "Apollo Task", stackSize);
            rc.setDaemon(true);
            return rc;
        }
    }) {

        @Override
        public void shutdown() {
            // we don't ever shutdown since we are shared..
        }

        @Override
        public List<Runnable> shutdownNow() {
            // we don't ever shutdown since we are shared..
            return Collections.emptyList();
        }
    };
}
