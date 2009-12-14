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

import org.apache.activemq.dispatch.internal.advanced.DispatchContext;
import org.apache.activemq.dispatch.internal.advanced.DispatcherThread;

/**
 * SelectableDispatchContext
 * <p>
 * Description:
 * </p>
 * 
 * @author cmacnaug
 * @version 1.0
 */
class SelectableDispatchContext extends DispatchContext {
    public static final boolean DEBUG = false;
    private SelectableChannel channel;
    private SelectionKey key;
    private int updateInterests = -1;
    private int readyOps = 0;

    SelectableDispatchContext(DispatcherThread thread, Runnable runnable, String name) {
        super(thread, runnable, true, name);

    }

    
    /**
     * This can be called to set a channel on which the Dispatcher will 
     * perform selection operations. The channel may be changed over time. 
     * 
     * This method may only be called from the provided {@link Dispatchable}
     * dispatch method, and is not thread safe.
     * 
     * @param channel The channel on which to select.
     * @throws ClosedChannelException If a closed chanel is provided
     */
    public void setChannel(SelectableChannel channel) throws ClosedChannelException {
        if (this.channel != channel) {
            if (isClosed()) {
                return;
            }
            int interests = 0;
            if (key != null) {
                interests = key.interestOps();
                key.cancel();
                key = null;
            }
            this.channel = channel;
            if (channel != null) {
                updateInterestOps(interests);
            }
        }
    }

    /**
     * May be overriden by subclass to additional work on dispatcher switch
     * 
     * @param oldDispatcher
     *            The old dispatcher
     * @param newDispatcher
     *            The new Dispatcher
     */
    protected void switchedDispatcher(DispatcherThread oldDispatcher, DispatcherThread newDispatcher) {
        if (DEBUG) {
            if (oldDispatcher == newDispatcher) {
                System.out.println(this + " switching to same dispatcher " + newDispatcher + Thread.currentThread());
            }
        }
        if (channel != null) {
            if (DEBUG)
                System.out.println(this + "Canceling key on dispatcher switch: " + oldDispatcher + newDispatcher);

            SelectionKey exising = channel.keyFor(((NIODispatcherThread)oldDispatcher).getSelector());
            if (exising != null) {
                updateInterests = exising.interestOps();
                exising.cancel();
            }
        }
    }

    public void processForeignUpdates() {
        synchronized (this) {
            if (channel != null) {
                if (updateInterests > 0) {
                    if (DEBUG)
                        debug(this + "processing foreign update interests: " + updateInterests);
                    updateInterestOps(updateInterests);
                    updateInterests = -1;
                }
            }
        }
        super.processForeignUpdates();
    }

    /**
     * This call updates the interest ops on which the dispatcher should select.
     * When an interest becomes ready, the dispatcher will call the {@link Dispatchable}'s 
     * dispatch() method. At that time, {@link #readyOps()} can be called to see what
     * interests are now ready. 
     * 
     * This method may only be called from {@link Dispatchable#dispatch()} and is not
     * threadsafe. If the {@link Dispatchable} wishes to change it's interest op it
     * must call {@link #requestDispatch()} so that they can be changed from the dispatch()
     * method.
     * 
     * @param interestOps The interest ops. 
     */
    public void updateInterestOps(int ops) {
        readyOps &= ~ops;
        if (key != null && key.isValid()) {
            key.interestOps(key.interestOps() | ops);
        } else {
            if (isClosed()) {
                return;
            }

            // Make sure that we don't already have an invalidated key for
            // this selector. If we do then do a select to get rid of the
            // key:
            SelectionKey existing = channel.keyFor( ((NIODispatcherThread)target).getSelector());
            if (existing != null && !existing.isValid()) {
                if (DEBUG)
                    debug(this + " registering existing invalid key:" + target + Thread.currentThread());
                try {
                    ((NIODispatcherThread)target).getSelector().selectNow();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
            if (DEBUG)
                System.out.println(this + " registering new key with interests: " + ops);
            try {
                key = channel.register( ((NIODispatcherThread)target).getSelector(), ops, this);
            } catch (ClosedChannelException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    /**
     * This call retrieves the operations that have become ready since the last call
     * to {@link #readyOps()}. Calling this method clears the ready ops. 
     * 
     * This method may only be called from {@link Dispatchable#dispatch()} and is not
     * threadsafe. 
     * 
     * @return the readyOps. 
     */
    public int readyOps() {
        return readyOps;
        /*
         * if (key == null || !key.isValid()) { return 0; } else { return
         * key.readyOps(); }
         */
    }

    public boolean onSelect() {
        readyOps = key.readyOps();
        key.interestOps(key.interestOps() & ~key.readyOps());
        synchronized (this) {
            if (!isLinked()) {
                target.execute(runnable, listPrio);
            }
        }

        // System.out.println(this + "onSelect " + key.readyOps() + "/" +
        return true;
    }

    public void close(boolean sync) {
        // actual close can only happen on the owning dispatch thread:
        if (target == DispatcherThread.CURRENT.get()) {

            if (key != null && key.isValid()) {
                // This will make sure that the key is removed
                // from the selector.
                key.cancel();
                try {
                    ((NIODispatcherThread)target).getSelector().selectNow();
                } catch (IOException e) {
                    if (DEBUG) {
                        debug("Error in close", e);
                    }
                }
            }
        }
        super.close(sync);
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
}
