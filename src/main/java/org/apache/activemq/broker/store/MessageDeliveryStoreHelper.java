package org.apache.activemq.broker.store;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.protobuf.AsciiBuffer;
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
import org.apache.activemq.flow.IFlowSink;
import org.apache.activemq.flow.ISinkController;
import org.apache.activemq.flow.SizeLimiter;
import org.apache.activemq.flow.ISinkController.FlowUnblockListener;


public class MessageDeliveryStoreHelper implements QueueStoreHelper<MessageDelivery>, Dispatchable, BrokerDatabase.MessageRestoreListener {

    private final BrokerDatabase database;
    private final AsciiBuffer queue;
    private final DispatchContext dispatchContext;
    private final ConcurrentLinkedQueue<RestoredMessage> restoredMsgs = new ConcurrentLinkedQueue<RestoredMessage>();
    private final IFlowRelay<MessageDelivery> restoreRelay;
    private final SizeLimiter<MessageDelivery> restoreLimiter;
    private final IFlowController<MessageDelivery> controller;
    private final FlowUnblockListener<MessageDelivery> unblockListener;
    private final IFlowSink<MessageDelivery> targetSink;

    private int RESTORE_BATCH_SIZE = 50;
    
    private AtomicBoolean started = new AtomicBoolean(false);
    private AtomicBoolean restoreComplete = new AtomicBoolean(false);
    private AtomicBoolean storeLoaded = new AtomicBoolean(false);

    private static enum State {
        STOPPED, RESTORING, RESTORED
    };

    private State state = State.RESTORING;

    MessageDeliveryStoreHelper(BrokerDatabase database, AsciiBuffer queueName, IFlowSink<MessageDelivery> sink, IDispatcher dispatcher) {
        this.database = database;
        this.queue = queueName;
        this.targetSink = sink;
        Flow flow = new Flow("MessageRestorer-" + queue, false);
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

    public void save(MessageDelivery elem, boolean flush) throws IOException {
        elem.persist(queue, !flush);
    }

    public boolean hasStoredElements() {
        return !restoreComplete.get();
    }

    public void startLoadingQueue() {
        database.restoreMessages(queue, 0, RESTORE_BATCH_SIZE, this);
    }

    public void stopLoadingQueue() {
        // TODO Auto-generated method stub
    }

    public boolean dispatch() {

        RestoredMessage restored = restoredMsgs.poll();
        if (restored == null || restoreComplete.get()) {
            return true;
        }

        if (controller.isSinkBlocked()) {
            if (controller.addUnblockListener(unblockListener)) {
                return true;
            }
        } else {
            try {
                targetSink.add(restored.getMessageDelivery(), controller);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        return false;
    }

    public void messagesRestored(Collection<RestoredMessage> msgs) {
        if (!msgs.isEmpty()) {
            restoredMsgs.addAll(msgs);
        } else {
            storeLoaded.set(true);
        }
        dispatchContext.requestDispatch();

        dispatchContext.requestDispatch();
    }
}
