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

import java.util.concurrent.*;

/**
 * <p>
 * Holds a singleton instance to a cached thread pool that can be used
 * to execute blocking tasks.  The tasks must be independent of each other
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class ApolloThreadPool {

    public static final int POOL_SIZE = Integer.parseInt(System.getProperty("apollo.thread.pool", "128"));

    public static final ThreadPoolExecutor INSTANCE = new ThreadPoolExecutor(POOL_SIZE, POOL_SIZE, 30, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(), new ThreadFactory() {
        public Thread newThread(Runnable r) {
            Thread rc = new Thread(r, "Apollo Blocking Task");
            rc.setDaemon(true);
            return rc;
        }
    });

    static {
        INSTANCE.allowCoreThreadTimeOut(true);
    }

}
