package org.apache.activemq.broker.store;

import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.activemq.queue.PersistentQueue;
import org.apache.activemq.queue.QueueStoreHelper;
import org.apache.activemq.queue.SingleFlowRelay;
import org.apache.activemq.broker.MessageDelivery;
import org.apache.activemq.broker.store.BrokerDatabase.RestoredMessage;
import org.apache.activemq.dispatch.IDispatcher;
import org.apache.activemq.dispatch.IDispatcher.DispatchContext;
import org.apache.activemq.dispatch.IDispatcher.Dispatchable;
import org.apache.activemq.flow.Flow;
import org.apache.activemq.flow.IFlowController;
import org.apache.activemq.flow.IFlowRelay;
import org.apache.activemq.flow.ISinkController;
import org.apache.activemq.flow.SizeLimiter;
import org.apache.activemq.flow.ISinkController.FlowUnblockListener;

public class MessageDeliveryStoreHelper implements QueueStoreHelper<MessageDelivery>, Dispatchable, BrokerDatabase.MessageRestoreListener {

    private final BrokerDatabase database;
    private final PersistentQueue<MessageDelivery> queue;
    private final DispatchContext dispatchContext;
    private final ConcurrentLinkedQueue<RestoredMessage> restoredMsgs = new ConcurrentLinkedQueue<RestoredMessage>();
    private final IFlowRelay<MessageDelivery> restoreRelay;
    private final SizeLimiter<MessageDelivery> restoreLimiter;
    private final IFlowController<MessageDelivery> controller;
    private final FlowUnblockListener<MessageDelivery> unblockListener;

    private int RESTORE_BATCH_SIZE = 50;
    
    private boolean restoreComplete;

    private static enum State {
        STOPPED, RESTORING, RESTORED
    };

    private State state = State.RESTORING;

    MessageDeliveryStoreHelper(BrokerDatabase database, PersistentQueue<MessageDelivery> queue, IDispatcher dispatcher) {
        this.database = database;
        this.queue = queue;
        Flow flow = new Flow("MessageRestorer-" + queue.getPeristentQueueName(), false);
        restoreLimiter = new SizeLimiter<MessageDelivery>(1000, 500) {
            @Override
            public int getElementSize(MessageDelivery msg) {
                return msg.getFlowLimiterSize();
            }
        };
        restoreRelay = new SingleFlowRelay<MessageDelivery>(flow, flow.getFlowName(), restoreLimiter);
        controller = restoreRelay.getFlowController(flow);
        dispatchContext = dispatcher.register(this, flow.getFlowName());

        unblockListener = new FlowUnblockListener<MessageDelivery>() {
            public void onFlowUnblocked(ISinkController<MessageDelivery> controller) {
                dispatchContext.requestDispatch();
            }
        };
    }

    public void delete(MessageDelivery elem, boolean flush) {
        elem.delete(queue);
    }

    public void save(MessageDelivery elem, boolean flush) {
        elem.persist(queue);
    }

    public boolean hasStoredElements() {
        // TODO Auto-generated method stub
        return false;
    }

    public void startLoadingQueue() {
        // TODO Auto-generated method stub
    }

    public void stopLoadingQueue() {
        // TODO Auto-generated method stub
    }

    public boolean dispatch() {

        RestoredMessage restored = restoredMsgs.poll();
        if (restored == null || restoreComplete) {
            return true;
        }

        if (controller.isSinkBlocked()) {
            if (controller.addUnblockListener(unblockListener)) {
                return true;
            }
        } else {
            queue.addFromStore(restored.getMessageDelivery(), controller);
        }

        return false;
    }

    public void messagesRestored(Collection<RestoredMessage> msgs) {
        synchronized (restoredMsgs) {
            if (!msgs.isEmpty()) {
                restoredMsgs.addAll(msgs);
            } else {

            }
        }

        dispatchContext.requestDispatch();
    }
}
