package org.apache.activemq.dispatch.internal.advanced;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;

import org.apache.activemq.util.list.LinkedNode;

/**
 * 
 */
public class DispatchContext extends LinkedNode<DispatchContext> implements Runnable {
    
    public static final ThreadLocal<DispatchContext> CURRENT = new ThreadLocal<DispatchContext>();

    // The target:
    protected final Runnable runnable;
    // The name of this context:
    final String label;

    // list prio can only be updated in the thread of of the owning
    // dispatcher
    protected int listPrio;

    // The update events are used to update fields in the dispatch context
    // from foreign threads:
    final UpdateEvent updateEvent[] = new UpdateEvent[2];

    final DispatchObserver tracker;
    protected DispatcherThread target;
    private DispatcherThread updateDispatcher = null;

    int priority;
    private boolean dispatchRequested = false;
    private boolean closed = false;
    final CountDownLatch closeLatch = new CountDownLatch(1);

    protected DispatchContext(DispatcherThread thread, Runnable runnable, boolean persistent, String label) {
        this.runnable = runnable;
        this.label = label;
        this.target = thread;
        if (persistent && target.dispatcher != null) {
            this.tracker = target.dispatcher.getLoadBalancer().createExecutionTracker((DispatchContext) this);
        } else {
            this.tracker = null;
        }
        updateEvent[0] = new UpdateEvent(this);
        updateEvent[1] = new UpdateEvent(this);
        if (persistent) {
            target.takeOwnership(this);
        }
    }


    /**
     * This can only be called by the owning dispatch thread:
     * 
     * @return False if the dispatchable has more work to do.
     */
    public final void run() {
        runnable.run();
    }

    public final void setTargetQueue(DispatcherThread newDispatcher) {
        synchronized (this) {

            // If we're already set to this dispatcher
            if (newDispatcher == target) {
                if (updateDispatcher == null || updateDispatcher == newDispatcher) {
                    return;
                }
            }

            updateDispatcher = (DispatcherThread) newDispatcher;
            if (DispatcherThread.DEBUG)
                System.out.println(getLabel() + " updating to " + updateDispatcher);

            target.onForeignUpdate(this);
        }

    }

    public void requestDispatch() {

        DispatcherThread callingDispatcher = DispatcherThread.CURRENT.get();
        if (tracker != null)
            tracker.onDispatch(callingDispatcher, DispatchContext.CURRENT.get());

        // Otherwise this is coming off another thread, so we need to
        // synchronize
        // to protect against ownership changes:
        synchronized (this) {
            // If the owner of this context is the calling thread, then
            // delegate to the dispatcher.
            if (target == callingDispatcher) {

                if (!target.running) {
                    // TODO In the event that the current dispatcher
                    // failed due to a runtime exception, we could
                    // try to switch to a new dispatcher.
                    throw new RejectedExecutionException();
                }
                if (!isLinked()) {
                    target.priorityQueue.add(this, listPrio);
                }
                return;
            }

            dispatchRequested = true;
            target.onForeignUpdate(this);
        }
    }

    public void updatePriority(int priority) {

        if (closed) {
            return;
        }

        priority = Math.min(priority, target.MAX_USER_PRIORITY);

        if (this.priority == priority) {
            return;
        }
        DispatcherThread callingDispatcher = DispatcherThread.CURRENT.get();

        // Otherwise this is coming off another thread, so we need to
        // synchronize to protect against ownership changes:
        synchronized (this) {
            if (closed) {
                return;
            }
            this.priority = priority;

            // If this is called by the owning dispatcher, then we go ahead
            // and update:
            if (target == callingDispatcher) {

                if (priority != listPrio) {

                    listPrio = priority;
                    // If there is a priority change relink the context
                    // at the new priority:
                    if (isLinked()) {
                        unlink();
                        target.priorityQueue.add(this, listPrio);
                    }
                }
                return;
            }

            target.onForeignUpdate(this);
        }

    }

    public void processForeignUpdates() {
        synchronized (this) {

            if (closed) {
                close(false);
                return;
            }

            if (updateDispatcher != null && updateDispatcher.takeOwnership(this)) {
                if (DispatcherThread.DEBUG) {
                    System.out.println("Assigning " + getLabel() + " to " + updateDispatcher);
                }

                if (target.removeDispatchContext(this)) {
                    dispatchRequested = true;
                }

                updateDispatcher.onForeignUpdate(this);
                switchedDispatcher(target, updateDispatcher);
                target = updateDispatcher;
                updateDispatcher = null;

            } else {
                updatePriority(priority);

                if (dispatchRequested) {
                    dispatchRequested = false;
                    requestDispatch();
                }
            }
        }
    }

    protected void switchedDispatcher(DispatcherThread oldDispatcher, DispatcherThread newDispatcher) {
    }

    public boolean isClosed() {
        return closed;
    }

    public void close(boolean sync) {
        DispatcherThread callingDispatcher = DispatcherThread.CURRENT.get();
        // System.out.println(this + "Closing");
        synchronized (this) {
            closed = true;
            // If the owner of this context is the calling thread, then
            // delegate to the dispatcher.
            if (target == callingDispatcher) {
                target.removeDispatchContext(this);
                closeLatch.countDown();
                return;
            }
        }

        target.onForeignUpdate(this);
        if (sync) {
            boolean interrupted = false;
            while (true) {
                try {
                    closeLatch.await();
                    break;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }

            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public DispatcherThread getTargetQueue() {
        return target;
    }

    public String getLabel() {
        return label;
    }

}