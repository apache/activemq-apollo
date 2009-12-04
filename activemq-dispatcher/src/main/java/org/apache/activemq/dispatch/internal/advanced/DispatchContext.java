package org.apache.activemq.dispatch.internal.advanced;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;

import org.apache.activemq.dispatch.internal.advanced.DispatcherThread.UpdateEvent;
import org.apache.activemq.util.list.LinkedNode;

/**
 * 
 */
class DispatchContext extends LinkedNode<DispatchContext> {

    private final DispatcherThread dispacher;
    // The target:
    private final Runnable runnable;
    // The name of this context:
    final String name;
    // list prio can only be updated in the thread of of the owning
    // dispatcher
    protected int listPrio;

    // The update events are used to update fields in the dispatch context
    // from foreign threads:
    final UpdateEvent updateEvent[];

    final DispatchObserver tracker;
    protected DispatcherThread currentOwner;
    private DispatcherThread updateDispatcher = null;

    int priority;
    private boolean dispatchRequested = false;
    private boolean closed = false;
    final CountDownLatch closeLatch = new CountDownLatch(1);

    protected DispatchContext(DispatcherThread dispatcherThread, Runnable runnable, boolean persistent, String name) {
        dispacher = dispatcherThread;
        this.runnable = runnable;
        this.name = name;
        this.currentOwner = (DispatcherThread) dispacher;
        if (persistent && dispacher.spi != null) {
            this.tracker = dispacher.spi.getLoadBalancer().createExecutionTracker((DispatchContext) this);
        } else {
            this.tracker = null;
        }
        updateEvent = createUpdateEvent();
        updateEvent[0] = dispacher.new UpdateEvent(this);
        updateEvent[1] = dispacher.new UpdateEvent(this);
        if (persistent) {
            currentOwner.takeOwnership(this);
        }
    }

    private final DispatcherThread.UpdateEvent[] createUpdateEvent() {
        return new DispatcherThread.UpdateEvent[2];
    }

    /**
     * Gets the execution tracker for the context.
     * 
     * @return the execution tracker for the context:
     */
    public DispatchObserver getExecutionTracker() {
        return tracker;
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
            if (newDispatcher == currentOwner) {
                if (updateDispatcher == null || updateDispatcher == newDispatcher) {
                    return;
                }
            }

            updateDispatcher = (DispatcherThread) newDispatcher;
            if (DispatcherThread.DEBUG)
                System.out.println(getName() + " updating to " + updateDispatcher);

            currentOwner.onForeignUpdate(this);
        }

    }

    public void requestDispatch() {

        DispatcherThread callingDispatcher = dispacher.getCurrentDispatcher();
        if (tracker != null)
            tracker.onDispatch(callingDispatcher, dispacher.getCurrentDispatchContext());

        // Otherwise this is coming off another thread, so we need to
        // synchronize
        // to protect against ownership changes:
        synchronized (this) {
            // If the owner of this context is the calling thread, then
            // delegate to the dispatcher.
            if (currentOwner == callingDispatcher) {

                if (!currentOwner.running) {
                    // TODO In the event that the current dispatcher
                    // failed due to a runtime exception, we could
                    // try to switch to a new dispatcher.
                    throw new RejectedExecutionException();
                }
                if (!isLinked()) {
                    currentOwner.priorityQueue.add(this, listPrio);
                }
                return;
            }

            dispatchRequested = true;
            currentOwner.onForeignUpdate(this);
        }
    }

    public void updatePriority(int priority) {

        if (closed) {
            return;
        }

        priority = Math.min(priority, dispacher.MAX_USER_PRIORITY);

        if (this.priority == priority) {
            return;
        }
        DispatcherThread callingDispatcher = dispacher.getCurrentDispatcher();

        // Otherwise this is coming off another thread, so we need to
        // synchronize to protect against ownership changes:
        synchronized (this) {
            if (closed) {
                return;
            }
            this.priority = priority;

            // If this is called by the owning dispatcher, then we go ahead
            // and update:
            if (currentOwner == callingDispatcher) {

                if (priority != listPrio) {

                    listPrio = priority;
                    // If there is a priority change relink the context
                    // at the new priority:
                    if (isLinked()) {
                        unlink();
                        currentOwner.priorityQueue.add(this, listPrio);
                    }
                }
                return;
            }

            currentOwner.onForeignUpdate(this);
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
                    System.out.println("Assigning " + getName() + " to " + updateDispatcher);
                }

                if (currentOwner.removeDispatchContext(this)) {
                    dispatchRequested = true;
                }

                updateDispatcher.onForeignUpdate(this);
                switchedDispatcher(currentOwner, updateDispatcher);
                currentOwner = updateDispatcher;
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

    /**
     * May be overriden by subclass to additional work on dispatcher switch
     * 
     * @param oldDispatcher The old dispatcher
     * @param newDispatcher The new Dispatcher
     */
    protected void switchedDispatcher(DispatcherThread oldDispatcher, DispatcherThread newDispatcher) {

    }

    public boolean isClosed() {
        return closed;
    }

    public void close(boolean sync) {
        DispatcherThread callingDispatcher = dispacher.getCurrentDispatcher();
        // System.out.println(this + "Closing");
        synchronized (this) {
            closed = true;
            // If the owner of this context is the calling thread, then
            // delegate to the dispatcher.
            if (currentOwner == callingDispatcher) {
                dispacher.removeDispatchContext(this);
                closeLatch.countDown();
                return;
            }
        }

        currentOwner.onForeignUpdate(this);
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

    public final String toString() {
        return getName();
    }

    public DispatcherThread getTargetQueue() {
        return currentOwner;
    }

    public String getName() {
        return name;
    }

}