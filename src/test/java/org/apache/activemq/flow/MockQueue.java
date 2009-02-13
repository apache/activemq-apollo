/**
 * 
 */
package org.apache.activemq.flow;

import java.util.HashMap;

import org.apache.activemq.flow.MockBroker.DeliveryTarget;
import org.apache.activemq.queue.IQueue;
import org.apache.activemq.queue.Mapper;
import org.apache.activemq.queue.PartitionedQueue;
import org.apache.activemq.queue.SharedPriorityQueue;
import org.apache.activemq.queue.SharedQueue;
import org.apache.activemq.queue.Subscription;

class MockQueue implements MockBroker.DeliveryTarget {

    HashMap<DeliveryTarget, Subscription<Message>> subs = new HashMap<DeliveryTarget, Subscription<Message>>();
    private Destination destination;
    private IQueue<Long, Message> queue;
    private MockBroker broker;
    
    private Mapper<Integer, Message> partitionMapper;
    private Mapper<Long, Message> keyExtractor;

    private IQueue<Long, Message> createQueue() {

        if (partitionMapper!=null) {
            PartitionedQueue<Integer, Long, Message> queue = new PartitionedQueue<Integer, Long, Message>() {
                @Override
                protected IQueue<Long, Message> cratePartition(Integer partitionKey) {
                    return createSharedFlowQueue();
                }
            };
            queue.setPartitionMapper(partitionMapper);
            queue.setResourceName(destination.getName());
            return queue;
        } else {
            return createSharedFlowQueue();
        }
    }

    private IQueue<Long, Message> createSharedFlowQueue() {
        if (MockBrokerTest.PRIORITY_LEVELS > 1) {
            PrioritySizeLimiter<Message> limiter = new PrioritySizeLimiter<Message>(100, 1, MockBrokerTest.PRIORITY_LEVELS);
            limiter.setPriorityMapper(Message.PRIORITY_MAPPER);
            SharedPriorityQueue<Long, Message> queue = new SharedPriorityQueue<Long, Message>(destination.getName(), limiter);
            queue.setKeyMapper(keyExtractor);
            queue.setAutoRelease(true);
            queue.setDispatcher(broker.getDispatcher());
            return queue;
        } else {
            SizeLimiter<Message> limiter = new SizeLimiter<Message>(100, 1);
            SharedQueue<Long, Message> queue = new SharedQueue<Long, Message>(destination.getName(), limiter);
            queue.setKeyMapper(keyExtractor);
            queue.setAutoRelease(true);
            queue.setDispatcher(broker.getDispatcher());
            return queue;
        }
    }

    public final void deliver(ISourceController<Message> source, Message msg) {
        queue.add(msg, source);
    }
    
    public final Destination getDestination() {
        return destination;
    }

    public final void addConsumer(final DeliveryTarget dt) {
        Subscription<Message> sub = new Subscription<Message>() {
            public boolean isPreAcquired() {
                return true;
            }

            public boolean matches(Message message) {
                return dt.match(message);
            }

            public boolean isRemoveOnDispatch() {
                return true;
            }

            public IFlowSink<Message> getSink() {
                return dt.getSink();
            }

            @Override
            public String toString() {
                return getSink().toString();
            }
        };
        subs.put(dt, sub);
        queue.addSubscription(sub);
    }

    public boolean removeSubscirption(final DeliveryTarget dt) {
        Subscription<Message> sub = subs.remove(dt);
        if (sub != null) {
            return queue.removeSubscription(sub);
        }
        return false;
    }

    public void start() throws Exception {
        queue = createQueue();
    }

    public void stop() throws Exception {
    }

    public IFlowSink<Message> getSink() {
        return queue;
    }

    public boolean match(Message message) {
        return true;
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

}