package org.apache.activemq.dispatch;

import java.util.LinkedList;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

public class TimerHeap {
    final TreeMap<Long, LinkedList<Runnable>> timers = new TreeMap<Long, LinkedList<Runnable>>();

    public final void add(Runnable runnable, long delay, TimeUnit timeUnit) {

        long nanoDelay = timeUnit.convert(delay, TimeUnit.NANOSECONDS);
        long eTime = System.nanoTime() + nanoDelay;
        LinkedList<Runnable> list = new LinkedList<Runnable>();
        list.add(runnable);

        LinkedList<Runnable> old = timers.put(eTime, list);
        if (old != null) {
            list.addAll(old);
        }
    }
    
    /**
     * Returns the time of the next scheduled event. 
     * @return -1 if there are no events, otherwise the time that the next timer should fire.
     */
    public final long timeToNext() {
        if(timers.isEmpty())
        {
            return -1;
        }
        else
        {
            return Math.max(0, timers.firstKey() - System.nanoTime());
        }
    }

    /**
     * Executes ready timers.
     */
    public final void executeReadyTimers() {
        LinkedList<Runnable> ready = null;
        if (timers.isEmpty()) {
            return;
        } else {
            long now = System.nanoTime();
            long first = timers.firstKey();
            if (first > now) {
                return;
            }
            ready = new LinkedList<Runnable>();

            while (first < now) {
                ready.addAll(timers.remove(first));
                if (timers.isEmpty()) {
                    break;
                }
                first = timers.firstKey();

            }
        }

        for (Runnable runnable : ready) {
            try {
                runnable.run();
            } catch (Throwable thrown) {
                thrown.printStackTrace();
            }
        }
    }
}
