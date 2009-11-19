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

package org.apache.activemq.syscall;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A Callback implementation which provides a Future interface.
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 *
 * @param <T>
 */
public class FutureCallback<T> implements Callback<T>, Future<T> {
        
        private CountDownLatch done = new CountDownLatch(1);
        volatile private Throwable exception;
        volatile private T result;

        public void onFailure(Throwable exception) {
            this.exception = exception;
            done.countDown();
        }

        public void onSuccess(T result) {
            this.result = result;
            done.countDown();
        }

        public T get() throws InterruptedException, ExecutionException {
            done.await();
            return internalGet();
        }

        public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            if ( done.await(timeout, unit) ) {
                return internalGet();
            }
            throw new TimeoutException();
        }
        
        public boolean isDone() {
            return done.getCount()==0;
        }
        
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        /**
         * This future cannot be canceled.  It always throws UnsupportedOperationException.
         * 
         * @throws UnsupportedOperationException 
         */
        public boolean isCancelled() {
            throw new UnsupportedOperationException();
        }
        
        private T internalGet() throws ExecutionException {
            if( exception!=null ) {
                throw new ExecutionException(exception);
            }
            return result;
        }
    }