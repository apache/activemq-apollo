package org.apache.activemq.dispatch.internal;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class BaseRetained {
    
    final protected AtomicInteger reatinCounter = new AtomicInteger(0);
    final protected ArrayList<Runnable> shutdownHandlers = new ArrayList<Runnable>();

    public void addShutdownWatcher(Runnable shutdownHandler) {
        synchronized(shutdownHandlers) {
            shutdownHandlers.add(shutdownHandler);
        }
    }
    
    public void retain() {
        if( reatinCounter.getAndIncrement() == 0 ) {
            startup();
        }
    }

    public void release() {
        if( reatinCounter.decrementAndGet()==0 ) {
            shutdown();
        }
    }

    /**
     * Subclasses should override if they want to do some startup processing. 
     */
    protected void startup() {
    }


    /**
     * Subclasses should override if they want to do clean up. 
     */
    protected void shutdown() {
        ArrayList<Runnable> copy;
        synchronized(shutdownHandlers) {
            copy = new ArrayList<Runnable>(shutdownHandlers);
            shutdownHandlers.clear();
        }
        for (Runnable runnable : copy) {
            runnable.run();
        }
    }

}
