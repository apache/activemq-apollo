/**
 * 
 */
package org.apache.activemq.queue.actor.perf;

import java.util.HashMap;

import org.apache.activemq.flow.Commands.Destination;
import org.apache.activemq.queue.IQueue;
import org.apache.activemq.queue.PersistencePolicy;
import org.apache.activemq.queue.Subscription;
import org.apache.activemq.util.Mapper;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class MockQueue implements DeliveryTarget {

    HashMap<DeliveryTarget, Subscription<Message>> subs = new HashMap<DeliveryTarget, Subscription<Message>>();
    private Destination destination;
    private IQueue<Long, Message> queue;
    private MockBroker broker;

    private Mapper<Integer, Message> partitionMapper;
    private Mapper<Long, Message> keyExtractor;
//    private final MockStoreAdapater store = new MockStoreAdapater();
    private static final PersistencePolicy<Message> NO_PERSISTENCE = new PersistencePolicy.NON_PERSISTENT_POLICY<Message>();
    
    public void add(Message msg, Runnable r) {
        throw new RuntimeException("please implement me.");
    }
    public boolean hasSelector() {
        return false;
    }
    public boolean match(Message message) {
        return true;
    }

//    private IQueue<Long, Message> createQueue() {
//
//        if (partitionMapper != null) {
//            PartitionedQueue<Long, Message> queue = new PartitionedQueue<Long, Message>(destination.getName().toString()) {
//                @Override
//                public IQueue<Long, Message> createPartition(int partitionKey) {
//                    return createSharedFlowQueue();
//                }
//            };
//            queue.setPartitionMapper(partitionMapper);
//            queue.setResourceName(destination.getName().toString());
//            queue.setStore(store);
//            queue.setPersistencePolicy(NO_PERSISTENCE);
//            queue.initialize(0, 0, 0, 0);
//            return queue;
//        } else {
//            return createSharedFlowQueue();
//        }
//    }
//
//    private IQueue<Long, Message> createSharedFlowQueue() {
//        PrioritySizeLimiter<Message> limiter = new PrioritySizeLimiter<Message>(100, 1, MockBrokerTest.PRIORITY_LEVELS);
//        limiter.setPriorityMapper(Message.PRIORITY_MAPPER);
//        SharedPriorityQueue<Long, Message> queue = new SharedPriorityQueue<Long, Message>(destination.getName().toString(), limiter);
//        queue.setKeyMapper(keyExtractor);
//        queue.setAutoRelease(true);
//        queue.setDispatcher(broker.getDispatcher());
//        queue.setStore(store);
//        queue.setPersistencePolicy(NO_PERSISTENCE);
//        queue.initialize(0, 0, 0, 0);
//        return queue;
//    }
//
//    public final void deliver(ISourceController<Message> source, Message msg) {
//        queue.add(msg, source);
//    }
//
    public final Destination getDestination() {
        return destination;
    }

    public final void addConsumer(final DeliveryTarget dt) {
        throw new RuntimeException("please implement me.");
//        Subscription<Message> sub = new Subscription<Message>() {
//
//            public boolean isBrowser() {
//                return false;
//            }
//
//            public boolean matches(Message message) {
//                return dt.match(message);
//            }
//
//            public boolean isRemoveOnDispatch(Message message) {
//                return true;
//            }
//
//            public boolean isExclusive() {
//                return false;
//            }
//
//            public IFlowSink<Message> getSink() {
//                return dt.getSink();
//            }
//
//            @Override
//            public String toString() {
//                return getSink().toString();
//            }
//
//            public boolean hasSelector() {
//                return dt.hasSelector();
//            }
//
//            public boolean offer(Message elem, ISourceController<?> controller, SubscriptionDelivery<Message> ackCallback) {
//                return getSink().offer(elem, controller);
//            }
//
//            public void add(Message elem, ISourceController<?> controller, SubscriptionDelivery<Message> ackCallback) {
//                getSink().add(elem, controller);
//            }
//        };
//        subs.put(dt, sub);
//        queue.addSubscription(sub);
    }
//
//    public boolean removeSubscirption(final DeliveryTarget dt) {
//        Subscription<Message> sub = subs.remove(dt);
//        if (sub != null) {
//            return queue.removeSubscription(sub);
//        }
//        return false;
//    }
//
    public void start() throws Exception {
//        queue = createQueue();
//        queue.start();
    }

    public void stop() throws Exception {
    }

    public MockBroker getBroker() {
        return broker;
    }

    public void setBroker(MockBroker broker) {
        this.broker = broker;
    }

    public Mapper<Integer, Message> getPartitionMapper() {
        return partitionMapper;
    }

    public void setPartitionMapper(Mapper<Integer, Message> partitionMapper) {
        this.partitionMapper = partitionMapper;
    }

    public Mapper<Long, Message> getKeyExtractor() {
        return keyExtractor;
    }

    public void setKeyExtractor(Mapper<Long, Message> keyExtractor) {
        this.keyExtractor = keyExtractor;
    }

    public void setDestination(Destination destination) {
        this.destination = destination;
    }
//
//    static final class MockStoreAdapater implements QueueStore<Long, Message> {
//
//        MockStoreAdapater() {
//
//        }
//
//        public final void deleteQueueElement(SaveableQueueElement<Message> elem) {
//
//        }
//
//        public final boolean isFromStore(Message elem) {
//            return false;
//        }
//
//        public final void persistQueueElement(SaveableQueueElement<Message> elem, ISourceController<?> controller, boolean delayable) {
//            // Noop;
//        }
//
//        public final void restoreQueueElements(QueueDescriptor queue, boolean recordsOnly, long firstSequence, long maxSequence, int maxCount, RestoreListener<Message> listener) {
//            throw new UnsupportedOperationException("Mock broker doesn't support persistence");
//        }
//
//        public final void addQueue(QueueDescriptor queue) {
//
//        }
//
//        public final void deleteQueue(QueueDescriptor queue) {
//
//        }
//
//    }

}