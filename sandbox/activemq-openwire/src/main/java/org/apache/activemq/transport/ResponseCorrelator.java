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
package org.apache.activemq.transport;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.command.Command;
import org.apache.activemq.command.ExceptionResponse;
import org.apache.activemq.command.Response;
import org.apache.activemq.util.IntSequenceGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Adds the incrementing sequence number to commands along with performing the
 * corelation of responses to requests to create a blocking request-response
 * semantics.
 * 
 * @version $Revision: 1.4 $
 */
public class ResponseCorrelator extends TransportFilter {

    private static final Logger LOG = LoggerFactory.getLogger(ResponseCorrelator.class);
    private final Map<Integer, RequestCallback> requestMap = new HashMap<Integer, RequestCallback>();
    private IntSequenceGenerator sequenceGenerator;
    private final boolean debug = LOG.isDebugEnabled();
    private IOException error;

    public ResponseCorrelator(Transport next) {
        this(next, new IntSequenceGenerator());
    }

    public ResponseCorrelator(Transport next, IntSequenceGenerator sequenceGenerator) {
        super(next);
        this.sequenceGenerator = sequenceGenerator;
    }

    @Override
    public void oneway(final Object o, final CompletionCallback callback) {
        next.getDispatchQueue().execute(new Runnable(){
            public void run() {
                Command command = (Command)o;
                command.setCommandId(sequenceGenerator.getNextSequenceId());
                command.setResponseRequired(false);
                next.oneway(command, callback);
            }
        });
    }

    public <T> void request(final Object o, final RequestCallback<T> responseCallback) throws IOException {
        next.getDispatchQueue().execute(new Runnable(){
            public void run() {
                Command command = (Command)o;
                command.setCommandId(sequenceGenerator.getNextSequenceId());
                command.setResponseRequired(true);
                requestMap.put(command.getCommandId(), responseCallback);
                oneway(command, null);
            }
        });
    }

    public Object request(final Object o) throws IOException {
        return request(o, -1);
    }

    public Object request(final Object o, final int timeout) throws IOException {
        final CountDownLatch done = new CountDownLatch(1);
        final Object[] result = new Object [2];
        request(o, new RequestCallback() {
            public void onCompletion(Object resp) {
                result[0] = resp;
                done.countDown();
            }
            public void onFailure(Throwable caught) {
                result[1] = caught;
                done.countDown();
            }
        });
        try {
            if( timeout < 0 ) {
                done.await();
            } else {
                if( !done.await(timeout, TimeUnit.MILLISECONDS) ) {
                    return null;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException();
        }
        if( result[1]!=null ) {
            if( result[1] instanceof IOException ) {
                throw (IOException)result[1];
            }
            throw new RuntimeException((Throwable)result[1]);
        }
        return result[0];
    }

    public void onCommand(Object o) {
        Command command = null;
        if (o instanceof Command) {
            command = (Command)o;
        } else {
            throw new ClassCastException("Object cannot be converted to a Command,  Object: " + o);
        }
        if (command.isResponse()) {
            Response response = (Response)command;
            RequestCallback callback = null;
            callback = requestMap.remove(Integer.valueOf(response.getCorrelationId()));
            if (callback != null) {
                callback.onCompletion(response);
            } else {
                if (debug) {
                    LOG.debug("Received unexpected response: {" + command + "}for command id: " + response.getCorrelationId());
                }
            }
        } else {
            getTransportListener().onCommand(command);
        }
    }

    /**
     * If an async exception occurs, then assume no responses will arrive for
     * any of current requests. Lets let them know of the problem.
     */
    public void onException(IOException error) {
        dispose(error);
        super.onException(error);
    }
    
    @Override
    public void stop() throws Exception {
        dispose(new IOException("Stopped."));
        super.stop();
    }

    private void dispose(IOException error) {
        ArrayList<RequestCallback> requests=null;
        synchronized(requestMap) {
            if( this.error==null) {
                this.error = error;
                requests = new ArrayList<RequestCallback>(requestMap.values());
                requestMap.clear();
            }
        }
        if( requests!=null ) {
            for (RequestCallback callback : requests) {
                callback.onFailure(error);
            }
        }
    }

    public IntSequenceGenerator getSequenceGenerator() {
        return sequenceGenerator;
    }

}
