/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.apollo.broker

import _root_.java.util.{ArrayList}
import _root_.org.apache.activemq.filter.{FilterException, BooleanExpression}
import _root_.scala.collection.JavaConversions._
import path.PathFilter

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait BrokerSubscription {

    def connect(consumer:ConsumerContext)

    def disconnect(consumer:ConsumerContext)

    def getDestination():Destination

}


/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class CompositeSubscription(val destination:Destination, val subscriptions:List[BrokerSubscription] ) extends BrokerSubscription {


  def connect(consumer:ConsumerContext) = {
    for (sub <- subscriptions) {
        sub.connect(consumer);
    }
  }

  def disconnect(consumer:ConsumerContext) = {
    for (sub <- subscriptions) {
        sub.disconnect(consumer);
    }
  }

  def getDestination() = destination

}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object WildcardQueueSubscription extends Log {

}
/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class WildcardQueueSubscription(val host:VirtualHost, val destination:Destination, val consumer:ConsumerContext) extends BrokerSubscription with QueueLifecyleListener with Logging {

  override protected def log = WildcardQueueSubscription

    var filter = PathFilter.parseFilter(destination.getName());
    val childSubs = new ArrayList[BrokerSubscription]();


    ///////////////////////////////////////////////////////////////////
    // BrokerSubscription interface implementation
    ///////////////////////////////////////////////////////////////////
    def connect(cc:ConsumerContext) = {
        assert(cc == consumer)
// TODO:
//        val domain = host.router.getDomain(Broker.QUEUE_DOMAIN);
//        val matches = domain.route(destination.getName(), null);
//        for (target <- matches) {
//            val queue = target.asInstanceOf[Queue]
//            var childSub = host.createSubscription(consumer, queue.destination);
//            childSubs.add(childSub);
//            childSub.connect(consumer);
//        }
        host.addDestinationLifecyleListener(this);
    }

    def disconnect(cc:ConsumerContext) = {
        assert(cc == consumer)
          host.removeDestinationLifecyleListener(this);
          for (childSub <- childSubs) {
              childSub.disconnect(cc);
          }
          childSubs.clear();
    }

    def getDestination() : Destination =  destination

    ///////////////////////////////////////////////////////////////////
    // QueueLifecyleListener interface implementation
    ///////////////////////////////////////////////////////////////////
    def onCreate(queue:Queue) = {
        if( filter.matches(queue.destination.getName()) ) {
            try {
                var childSub = host.createSubscription(consumer, queue.destination);
                childSubs.add(childSub);
                childSub.connect(consumer);
            } catch {
              case e:Exception=>
                warn("Could not create dynamic subscription to "+queue.destination+": "+e);
                debug("Could not create dynamic subscription to "+queue.destination+": ", e);
            }
        }
    }

    def onDestroy(queue:Queue ) = {
    }

}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class TopicSubscription { // extends BrokerSubscription with DeliveryTarget {
  def matches(message:Delivery) = false
  def deliver(message:Delivery) = {}
  def connect(consumer:ConsumerContext) = {}
  def disconnect(consumer:ConsumerContext) = {}
  def getDestination():Destination = null

//	static final boolean USE_PERSISTENT_QUEUES = true;
//
//    protected final BooleanExpression selector;
//    protected final Destination destination;
//    protected Subscription<MessageDelivery> connectedSub;
//    private final VirtualHost host;
//
//    //TODO: replace this with a base interface for queue which also support non persistent use case.
//	private IFlowQueue<MessageDelivery> queue;
//
//    TopicSubscription(VirtualHost host, Destination destination, BooleanExpression selector) {
//        this.host = host;
//        this.selector = selector;
//        this.destination = destination;
//    }
//
//    @Override
//    public String toString() {
//        return IntrospectionSupport.toString(this);
//    }
//
//    /*
//     * (non-Javadoc)
//     *
//     * @see
//     * org.apache.activemq.broker.DeliveryTarget#deliver(org.apache.activemq
//     * .broker.MessageDelivery, org.apache.activemq.flow.ISourceController)
//     */
//    public final void deliver(MessageDelivery message, ISourceController<?> source) {
//        if (matches(message)) {
//            queue.add(message, source);
//        }
//    }
//
//    /*
//     * (non-Javadoc)
//     *
//     * @see org.apache.activemq.broker.DeliveryTarget#hasSelector()
//     */
//    public boolean hasSelector() {
//        return selector != null;
//    }
//
//    public synchronized void connect(final ConsumerContext subscription) throws UserAlreadyConnectedException {
//        if (this.connectedSub == null) {
//        	if( subscription.isPersistent() ) {
//        		queue = createPersistentQueue(subscription);
//        	} else {
//        		queue = createNonPersistentQueue(subscription);
//        	}
//    		queue.start();
//
//        	this.connectedSub = subscription;
//        	this.queue.addSubscription(connectedSub);
//    		this.host.getRouter().bind(destination, this);
//        } else if (connectedSub != subscription) {
//            throw new UserAlreadyConnectedException();
//        }
//    }
//
//    private IFlowQueue<MessageDelivery> createNonPersistentQueue(final ConsumerContext subscription) {
//		Flow flow = new Flow(subscription.getResourceName(), false);
//		String name = subscription.getResourceName();
//		IFlowLimiter<MessageDelivery> limiter = new SizeLimiter<MessageDelivery>(100, 50);
//		ExclusiveQueue<MessageDelivery> queue = new ExclusiveQueue<MessageDelivery>(flow, name, limiter);
//		queue.setDrain( new QueueDispatchTarget<MessageDelivery>() {
//            public void drain(MessageDelivery elem, ISourceController<MessageDelivery> controller) {
//                subscription.add(elem, controller);
//            }
//        });
//		return queue;
//	}
//
//	private IFlowQueue<MessageDelivery> createPersistentQueue(ConsumerContext subscription) {
//        ExclusivePersistentQueue<Long, MessageDelivery> queue = host.getQueueStore().createExclusivePersistentQueue();
//        return queue;
//	}
//
//    @SuppressWarnings("unchecked")
//	private void destroyPersistentQueue(IFlowQueue<MessageDelivery> queue) {
//    	ExclusivePersistentQueue<Long, MessageDelivery> pq = (ExclusivePersistentQueue<Long, MessageDelivery>) queue;
//		host.getQueueStore().deleteQueue(pq.getDescriptor());
//	}
//
//	public synchronized void disconnect(final ConsumerContext subscription) {
//        if (connectedSub != null && connectedSub == subscription) {
//    		this.host.getRouter().unbind(destination, this);
//    		this.queue.removeSubscription(connectedSub);
//    		this.connectedSub = null;
//
//    		queue.stop();
//        	if( USE_PERSISTENT_QUEUES ) {
//        		destroyPersistentQueue(queue);
//        	}
//    		queue=null;
//        }
//    }
//
//
//
//	public boolean matches(MessageDelivery message) {
//        if (selector == null) {
//            return true;
//        }
//
//        MessageEvaluationContext selectorContext = message.createMessageEvaluationContext();
//        selectorContext.setDestination(destination);
//        try {
//            return (selector.matches(selectorContext));
//        } catch (FilterException e) {
//            e.printStackTrace();
//            return false;
//        }
//    }
//
//    /*
//     * (non-Javadoc)
//     *
//     * @see org.apache.activemq.broker.BrokerSubscription#getDestination()
//     */
//    public Destination getDestination() {
//        return destination;
//    }


}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class DurableSubscription(val host:VirtualHost, val destination:Destination, val selector:BooleanExpression) { // extends BrokerSubscription with DeliveryTarget {

//    private final IQueue<Long, MessageDelivery> queue;
//    private Subscription<MessageDelivery> connectedSub;
    var started = false;
//  TODO:
//    this.host.router.bind(destination, this);

    /* (non-Javadoc)
     * @see org.apache.activemq.broker.BrokerSubscription#getDestination()
     */
    def getDestination() = destination

    /* (non-Javadoc)
     * @see org.apache.activemq.broker.DeliveryTarget#deliver(org.apache.activemq.broker.MessageDelivery, org.apache.activemq.flow.ISourceController)
     */
    def deliver(message:Delivery ) = {
//        TODO:
//        queue.add(message, source);
    }

    def connect(subscription:ConsumerContext) = {
//        TODO:
//        if (this.connectedSub == null) {
//            this.connectedSub = subscription;
//            queue.addSubscription(connectedSub);
//        } else if (connectedSub != subscription) {
//            throw new UserAlreadyConnectedException();
//        }
    }

  def disconnect(subscription:ConsumerContext) = {
//        TODO:
//        if (connectedSub != null && connectedSub == subscription) {
//            queue.removeSubscription(connectedSub);
//            connectedSub = null;
//        }
    }

    def matches(message:Delivery) = {
        if (selector != null) {
          var selectorContext = message.message.messageEvaluationContext
          selectorContext.setDestination(destination);
          try {
              (selector.matches(selectorContext));
          } catch {
            case e:FilterException=>
              e.printStackTrace();
              false;
          }
        } else {
            true;
        }

    }

}
