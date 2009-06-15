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
package org.apache.activemq.transport.pipe;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Pipe<E> {
    private final LinkedBlockingQueue<E> in;
    private final LinkedBlockingQueue<E> out;
    private boolean connected = false;
    private ReadReadyListener<E> rListener;
    private Pipe<E> peer;

    public static final short ASYNC = 0;
    public static final short BLOCKING = 1;
    public static final short POLLING = 2;
    private int mode = BLOCKING;

    public interface ReadReadyListener<E> {
        public void onReadReady(Pipe<E> pipe);
    }

    public Pipe(int capacity) {
        this(new LinkedBlockingQueue<E>(capacity), new LinkedBlockingQueue<E>(capacity));
    }
    
    public Pipe() {
        this(new LinkedBlockingQueue<E>(), new LinkedBlockingQueue<E>());
    }

    private Pipe(LinkedBlockingQueue<E> in, LinkedBlockingQueue<E> out) {
        this.in = in;
        this.out = out;
    }

    public void setMode(short mode) {
        this.mode = mode;
    }

    public synchronized Pipe<E> connect() {
        if (connected) {
            throw new IllegalStateException("Already Connected");
        }
        Pipe<E> ret = new Pipe<E>(out, in);
        peer = ret;
        ret.peer = this;
        ret.connected = true;
        connected = true;
        return ret;
    }

    public void write(E o) throws InterruptedException {
        if (peer.mode == BLOCKING) {
            out.put(o);
            return;
        }
        ReadReadyListener<E> rl = null;
        synchronized (out) {
            out.put(o);
            rl = peer.rListener;
            peer.rListener = null;
        }
        if (rl != null) {
            rl.onReadReady(this);
        }
    }

    public void setReadReadyListener(ReadReadyListener<E> listener) {
        if (in.peek() != null) {
            listener.onReadReady(this);
        }
        synchronized (in) {
            if (in.peek() == null) {
                rListener = listener;
                return;
            }
        }
        listener.onReadReady(this);
    }

    public E read() throws InterruptedException {
        return in.take();
    }

    public E poll() {
        return in.poll();
    }

    public E poll(long time, TimeUnit unit) throws InterruptedException {
        return in.poll(time, unit);
    }

    public boolean offer(E arg0, long arg1, TimeUnit arg2) throws InterruptedException {
        return out.offer(arg0, arg1, arg2);
    }

    public boolean offer(E arg0) {
        return out.offer(arg0);
    }

}
