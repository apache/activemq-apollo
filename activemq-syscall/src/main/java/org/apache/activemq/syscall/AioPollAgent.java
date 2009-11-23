package org.apache.activemq.syscall;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.syscall.jni.Time.timespec;

import static java.util.concurrent.TimeUnit.*;
import static org.apache.activemq.syscall.jni.CLibrary.*;
import static org.apache.activemq.syscall.jni.PosixAIO.*;
import static org.apache.activemq.syscall.jni.Time.timespec.*;

public class AioPollAgent {
    
    private static final int ASYNC_THREAD_SHUTDOWN_DELAY = 1000;
    public static final AioPollAgent MAIN_POLL_AGENT = new AioPollAgent();
    public static AioPollAgent getMainPollAgent() {
        return MAIN_POLL_AGENT;
    }

    long timeoutp;
    LinkedHashMap<Long, Callback<Long>> callbackMap = new LinkedHashMap<Long, Callback<Long>>();
    private Thread thread;
    
    public void setTimeOut(long value, TimeUnit unit) {
        synchronized (this) {
            if( timeoutp == NULL ) {
                timeoutp = malloc(timespec.SIZEOF);
                if( timeoutp == NULL ) {
                    throw new OutOfMemoryError();
                }
            }
            memmove(timeoutp, timespec(value, unit), timespec.SIZEOF);
        }
    }
    
    static public timespec timespec(long value, TimeUnit unit) {
        return timespec(unit.toMillis(value));
    }
    
    static public timespec timespec(long ms) {
        timespec t = new timespec();
        t.tv_sec = ms/1000;
        ms = ms%1000;
        t.tv_nsec = 1000000*ms; // 50 milli seconds
        return t;
    }
    
    public void watch(long aiocbp, Callback<Long> callback) {
        assert aiocbp!=NULL;
        synchronized (this) {
            if( callbackMap.isEmpty() && thread==null) {
                if( timeoutp==NULL ) {
                    // Default the timeout..
                    setTimeOut(10, MILLISECONDS);
                }
                thread = new Thread("AioPollAgent") {
                    public void run() {
                        process();
                    }
                };
                thread.setDaemon(false);
                thread.setPriority(Thread.MAX_PRIORITY);
                thread.start();
            }
            callbackMap.put(aiocbp, callback);
            notify();
        }
    }

    private void process() {
        long[] aiocbps;
        while ((aiocbps = dequeueBlocks())!=null ) {
            process(aiocbps);
        }
    }

    private long[] dequeueBlocks() {
        long blocks[];
        synchronized (this) {
            if( callbackMap.isEmpty() ) {
                try {
                    wait(ASYNC_THREAD_SHUTDOWN_DELAY);
                    if( callbackMap.isEmpty() ) {
                        thread=null;
                        return null;
                    }
                } catch (InterruptedException e) {
                    thread=null;
                    return null;
                } 
            }
            Set<Long> keys = callbackMap.keySet();
            blocks = new long[keys.size()];
            int i=0;
            for (Long value : keys) {
                blocks[i++]=value;
            }
        }
        return blocks;
    }

    private void process(long[] aiocbps) {
        int rc = aio_suspend(aiocbps, aiocbps.length, timeoutp);
        if (rc == 0) {
            for (int i = 0; i < aiocbps.length; i++) {
                rc = aio_error(aiocbps[i]);
                if( rc==EINPROGRESS )
                    continue;
                
                // The io has now completed.. free up the memory allocated for 
                // the aio control block, a remove it from the callback map
                Callback<Long> callback = removeBlock(aiocbps[i]);
                free(aiocbps[i]);
                
                // Let the callback know that the IO completed.
                try {
                    if( rc==0 ) {
                        callback.onSuccess(aio_return(aiocbps[i]));
                    } else {
                        callback.onFailure(new IOException(string(strerror(errno()))));
                    }
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private Callback<Long> removeBlock(long aiocbp) {
        synchronized (this) {
            return callbackMap.remove(aiocbp);
        }
    }

    protected void finalize() throws Throwable {
        synchronized(this) {
            if ( timeoutp!=NULL ) {
                free(timeoutp);
                timeoutp=NULL;
            }
        }
    }
    
}
