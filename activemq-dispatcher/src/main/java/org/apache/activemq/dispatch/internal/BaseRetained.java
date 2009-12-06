package org.apache.activemq.dispatch.internal;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class BaseRetained {
    
    final protected AtomicInteger reatinCounter = new AtomicInteger(1);
    final protected AtomicReference<Runnable> shutdownHandler = new AtomicReference<Runnable>();

    public void setShutdownHandler(Runnable finalizer) {
        this.shutdownHandler.set(finalizer);
    }
    
    public void retain() {
        int prev = reatinCounter.getAndIncrement();
        assert prev!=0;
    }

    public void release() {
        if( reatinCounter.decrementAndGet()==0 ) {
            Runnable value = shutdownHandler.getAndSet(null);
            if( value!=null ) {
                value.run();
            }
        }
    }

}
