/**
 * 
 */
package org.apache.activemq.flow;

import java.util.HashMap;

import org.apache.activemq.flow.MockBrokerTest.DeliveryTarget;
import org.apache.activemq.queue.IQueue;
import org.apache.activemq.queue.PartitionedQueue;
import org.apache.activemq.queue.SharedPriorityQueue;
import org.apache.activemq.queue.SharedQueue;
import org.apache.activemq.queue.Subscription;

class MockQueue implements MockBrokerTest.DeliveryTarget {

    private final MockBrokerTest mockBrokerTest;
    HashMap<DeliveryTarget, Subscription<Message>> subs = new HashMap<DeliveryTarget, Subscription<Message>>();
    private final Destination destination;
    private final IQueue<Long, Message> queue;
    private final MockBroker broker;

    MockQueue(MockBrokerTest mockBrokerTest, MockBroker broker, Destination destination) {
        this.mockBrokerTest = mockBrokerTest;
        this.broker = broker;
        this.destination = destination;
        this.queue = createQueue();
        broker.router.bind(this, destination);
    }

    private IQueue<Long, Message> createQueue() {

        if (this.mockBrokerTest.usePartitionedQueue) {
            PartitionedQueue<Integer, Long, Message> queue = new PartitionedQueue<Integer, Long, Message>() {
                @Override
                protected IQueue<Long, Message> cratePartition(Integer partitionKey) {
                    return createSharedFlowQueue();
                }
            };
            queue.setPartitionMapper(this.mockBrokerTest.partitionMapper);
            queue.setResourceName(destination.getName());
            return queue;
        } else {
            return createSharedFlowQueue();
        }
    }

    private IQueue<Long, Message> createSharedFlowQueue() {
        if (broker.priorityLevels > 1) {
            PrioritySizeLimiter<Message> limiter = new PrioritySizeLimiter<Message>(100, 1, broker.priorityLevels);
            limiter.setPriorityMapper(Message.PRIORITY_MAPPER);
            SharedPriorityQueue<Long, Message> queue = new SharedPriorityQueue<Long, Message>(destination.getName(), limiter);
            queue.setKeyMapper(this.mockBrokerTest.keyExtractor);
            queue.setAutoRelease(true);
            queue.setDispatcher(broker.dispatcher);
            return queue;
        } else {
            SizeLimiter<Message> limiter = new SizeLimiter<Message>(100, 1);
            SharedQueue<Long, Message> queue = new SharedQueue<Long, Message>(destination.getName(), limiter);
            queue.setKeyMapper(this.mockBrokerTest.keyExtractor);
            queue.setAutoRelease(true);
            queue.setDispatcher(broker.dispatcher);
            return queue;
        }
    }

    public final void deliver(ISourceController<Message> source, Message msg) {

        queue.add(msg, source);
    }

    public String getSelector() {
        return null;
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
    }

    public void stop() throws Exception {
    }

    public IFlowSink<Message> getSink() {
        return queue;
    }

    public boolean match(Message message) {
        return true;
    }

}