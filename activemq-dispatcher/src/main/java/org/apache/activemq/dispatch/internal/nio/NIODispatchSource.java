/**************************************************************************************
 * Copyright (C) 2009 Progress Software, Inc. All rights reserved.                    *
 * http://fusesource.com                                                              *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the AGPL license      *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package org.apache.activemq.dispatch.internal.nio;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.dispatch.DispatchSource;
import org.apache.activemq.dispatch.internal.AbstractDispatchObject;

/**
 * SelectableDispatchContext
 * <p>
 * Description:
 * </p>
 * 
 * @author cmacnaug
 * @version 1.0
 */
public class NIODispatchSource extends AbstractDispatchObject implements DispatchSource {
    public static final boolean DEBUG = false;
    private final AtomicLong suspendCounter = new AtomicLong(1);
    private SelectableChannel channel;
    private SelectionKey key;
    private int interestOps = 0;
    private int readyOps = 0;
    private NIOSourceHandler sourceHandler;
    private final EventHandler eventHandler = new EventHandler();
    private Runnable cancelHandler;

    public NIODispatchSource() {
        super.retain();
    }

    /**
     * This can be called to set a channel on which the Dispatcher will perform
     * selection operations. The channel may be changed over time, but only from
     * the registered event handler.
     * 
     * @param channel
     *            The channel on which to select.
     * @throws ClosedChannelException
     *             If a closed channel is provided
     */
    public void setChannel(SelectableChannel channel) throws ClosedChannelException {
        if (this.channel != channel) {
            if (super.isShutdown()) {
                return;
            }
            int interests = interestOps;
            if (key != null) {
                interests |= key.interestOps();
                key.cancel();
                key = null;
            }
            this.channel = channel;
            if (channel != null) {
                setMask(interests);
            }
        }
    }

    /**
     * Set's the handler for this source, this may only be called from the event
     * handler
     * 
     * @param newHandler
     *            The new handler for the source
     */
    protected void setHandler(NIOSourceHandler newHandler) {

        if (sourceHandler != newHandler) {
            if (channel != null) {
                if (DEBUG)
                    System.out.println(this + "Canceling key on source handler switch: " + sourceHandler + "-" + newHandler);

                SelectionKey exising = channel.keyFor(sourceHandler.getSelector());
                if (exising != null) {
                    interestOps = exising.interestOps();
                    exising.cancel();
                }
            }
        } else if (DEBUG) {
            if (this.sourceHandler == newHandler) {
                System.out.println(this + " switching to new source handler " + sourceHandler + Thread.currentThread());
            }
        }
    }

    /**
     * This call updates the interest ops on which the dispatcher should select.
     * When an interest becomes ready, the eventHandler for the source will be
     * called. At that time the EventHandler can call {@link #getData()} to see
     * which ops are ready.
     * 
     * The masks may be changed over time, but it is only legal to do so from
     * the supplied eventHandler.
     * 
     * @param interestOps
     *            The interest ops.
     */
    public void setMask(long ops) {

        readyOps &= ~ops;
        if (key != null && key.isValid()) {
            key.interestOps(key.interestOps() | (int) ops);
        } else {

            if (isShutdown() || suspendCounter.get() > 0) {
                interestOps |= (int) ops;
                return;
            }

            // Make sure that we don't already have an invalidated key for
            // this selector. If we do then do a select to get rid of the
            // key:
            SelectionKey existing = channel.keyFor(sourceHandler.getSelector());
            if (existing != null && !existing.isValid()) {
                if (DEBUG)
                    debug(this + " registering existing invalid key:" + sourceHandler + Thread.currentThread());
                try {
                    sourceHandler.getSelector().selectNow();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
            if (DEBUG)
                System.out.println(this + " registering new key with interests: " + interestOps);
            try {
                key = channel.register(sourceHandler.getSelector(), interestOps, this);
            } catch (ClosedChannelException e) {
                throw new IllegalStateException("Channel was closed", e);
            }
        }
    }

    /**
     * This call retrieves the operations that have become ready since the last
     * call to {@link #readyOps()}. Calling this method clears the ready ops.
     * 
     * It is only legal to call this method from the supplied eventHandler.
     * 
     * @return the readyOps.
     */
    public long getData() {
        return readyOps;
        /*
         * if (key == null || !key.isValid()) { return 0; } else { return
         * key.readyOps(); }
         */
    }

    final boolean onSelect() {
        readyOps = key.readyOps();
        key.interestOps(key.interestOps() & ~key.readyOps());
        if (suspendCounter.get() <= 0) {
            eventHandler.addToQueue();
        }

        // System.out.println(this + "onSelect " + key.readyOps() + "/" +
        return key.interestOps() == 0;
    }

    @Override
    protected void shutdown() {
        // actual close can only happen on the owning dispatch thread:
        if (key != null && key.isValid()) {
            // This will make sure that the key is removed
            // from the selector.
            key.cancel();
            try {
                sourceHandler.getSelector().selectNow();
            } catch (IOException e) {
                if (DEBUG) {
                    debug("Error in close", e);
                }
            }

            if (cancelHandler != null) {
                cancelHandler.run();
            }
        }
    }

    @Override
    public void retain() {
        throw new UnsupportedOperationException("Sources are retained until canceled");
    }

    @Override
    public void release() {
        throw new UnsupportedOperationException("Sources must be release via cancel");
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.dispatch.DispatchObject#resume()
     */
    public void resume() {
        long c = suspendCounter.decrementAndGet();
        if (c < 0) {
            throw new IllegalStateException();
        }
        if (c == 0 && readyOps != 0) {
            eventHandler.addToQueue();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.dispatch.DispatchObject#suspend()
     */
    public void suspend() {
        suspendCounter.incrementAndGet();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.dispatch.DispatchSource#cancel()
     */
    public void cancel() {
        getTargetQueue().dispatchAsync(new Runnable() {
            public void run() {
                NIODispatchSource.super.release();
            }
        });
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.dispatch.DispatchSource#getMask()
     */
    public long getMask() {
        return interestOps;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.dispatch.DispatchSource#isCanceled()
     */
    public boolean isCanceled() {
        return isShutdown();
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.activemq.dispatch.DispatchSource#setCancelHandler(java.lang
     * .Runnable)
     */
    public void setCancelHandler(Runnable cancelHandler) {
        this.cancelHandler = cancelHandler;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.activemq.dispatch.DispatchSource#setEventHandler(java.lang
     * .Runnable)
     */
    public void setEventHandler(Runnable eventHandler) {
        this.eventHandler.setUserHandler(eventHandler);
    }

    protected void debug(String str) {
        System.out.println(str);
    }

    protected void debug(String str, Throwable thrown) {
        if (str != null) {
            System.out.println(str);
        }
        if (thrown != null) {
            thrown.printStackTrace();
        }
    }

    private class EventHandler implements Runnable {

        private Runnable userHandler;
        private AtomicBoolean queued = new AtomicBoolean(false);
        private AtomicBoolean running = new AtomicBoolean(false);
        
        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Runnable#run()
         */
        public void run() {
            running.set(true);
            queued.set(false);
            try {
                userHandler.run();
            } finally {
                running.set(false);
            }
        }
        
        public void addToQueue()
        {
            if(!queued.compareAndSet(false, true))
            {
                getTargetQueue().dispatchAsync(this);
            }
        }
        
        public boolean isRunning()
        {
            return running.get();
        }
        
        public boolean isQueued()
        {
            return queued.get();
        }
        
        public void setUserHandler(Runnable userHandler)
        {
            this.userHandler = userHandler;
        }
    }

}
