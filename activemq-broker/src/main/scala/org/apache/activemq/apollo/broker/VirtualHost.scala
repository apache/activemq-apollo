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
package org.apache.activemq.apollo.broker;

import _root_.java.util.{ArrayList, HashMap}
import _root_.org.apache.activemq.Service
import _root_.java.lang.{String}
import _root_.org.fusesource.hawtdispatch.{ScalaDispatch, DispatchQueue}
import _root_.scala.collection.JavaConversions._
import _root_.scala.reflect.BeanProperty
import path.PathFilter
import org.fusesource.hawtbuf.AsciiBuffer
import org.apache.activemq.apollo.dto.VirtualHostDTO
import _root_.org.fusesource.hawtdispatch.ScalaDispatch._

import ReporterLevel._
import org.apache.activemq.broker.store.{Store}
import org.fusesource.hawtbuf.proto.WireFormat
import org.apache.activemq.apollo.store.QueueRecord

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object VirtualHost extends Log {

  val destination_parser_options = new ParserOptions
  destination_parser_options.queuePrefix = new AsciiBuffer("queue:")
  destination_parser_options.topicPrefix = new AsciiBuffer("topic:")
  destination_parser_options.tempQueuePrefix = new AsciiBuffer("temp-queue:")
  destination_parser_options.tempTopicPrefix = new AsciiBuffer("temp-topic:")

  /**
   * Creates a default a configuration object.
   */
  def default() = {
    val rc = new VirtualHostDTO
    rc.id = "default"
    rc.enabled = true
    rc.hostNames.add("localhost")
    rc
  }

  /**
   * Validates a configuration object.
   */
  def validate(config: VirtualHostDTO, reporter:Reporter):ReporterLevel = {
     new Reporting(reporter) {
      if( config.hostNames.isEmpty ) {
        error("Virtual host must be configured with at least one host name.")
      }
    }.result
  }
  
}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class VirtualHost(val broker: Broker) extends BaseService with DispatchLogging {
  import VirtualHost._
  
  override protected def log = VirtualHost
  override val dispatchQueue:DispatchQueue = ScalaDispatch.createQueue("virtual-host");

  var config:VirtualHostDTO = _
  private val queueStore = new BrokerQueueStore()
  private val queues = new HashMap[AsciiBuffer, Queue]()
  private val durableSubs = new HashMap[String, DurableSubscription]()
  val router = new Router(this)

  var names:List[String] = Nil;
  def setNamesArray( names:ArrayList[String]) = {
    this.names = names.toList
  }

  var store:Store = null
  var transactionManager:TransactionManagerX = new TransactionManagerX

  var protocols = Map[AsciiBuffer, WireFormat]()

  override def toString = if (config==null) "virtual-host" else "virtual-host: "+config.id

  /**
   * Validates and then applies the configuration.
   */
  def configure(config: VirtualHostDTO, reporter:Reporter) = ^{
    if ( validate(config, reporter) < ERROR ) {
      this.config = config

      if( serviceState.isStarted ) {
        // TODO: apply changes while he broker is running.
        reporter.report(WARN, "Updating virtual host configuration at runtime is not yet supported.  You must restart the broker for the change to take effect.")

      }
    }
  } |>>: dispatchQueue


  override protected def _start(onCompleted:Runnable):Unit = {
    if( store!=null ) {
      store.start();
      store.listQueues { ids =>
        for( id <- ids) {
          store.getQueueStatus(id) { x =>
            x match {
              case Some(info)=>
              dispatchQueue ^{
                val dest = DestinationParser.parse(info.record.name , destination_parser_options)
                if( dest.getDomain == Domain.QUEUE_DOMAIN ) {

                  val queue = new Queue(this, dest, id)
                  queue.first_seq = info.first
                  queue.last_seq = info.last
                  queue.message_seq_counter = info.last+1
                  queue.count = info.count

                  queues.put(info.record.name, queue)
                }
              }
              case _ =>
            }
          }
        }
      }
    }


    //Recover transactions:
    transactionManager.virtualHost = this
    transactionManager.loadTransactions();
    onCompleted.run
  }


  override protected def _stop(onCompleted:Runnable):Unit = {

//    TODO:
//      val tmp = new ArrayList[Queue](queues.values())
//      for (queue <-  tmp) {
//        queue.shutdown
//      }

// TODO:
//        ArrayList<IQueue<Long, MessageDelivery>> durableQueues = new ArrayList<IQueue<Long,MessageDelivery>>(queueStore.getDurableQueues());
//        done = new RunnableCountDownLatch(durableQueues.size());
//        for (IQueue<Long, MessageDelivery> queue : durableQueues) {
//            queue.shutdown(done);
//        }
//        done.await();

    if( store!=null ) {
      store.stop();
    }
    onCompleted.run
  }

  def getQueue(destination:Destination)(cb: (Queue)=>Unit ) = ^{
    if( !serviceState.isStarted ) {
      error("getQueue can only be called while the service is running.")
      cb(null)
    } else {
      var queue = queues.get(destination);
      if( queue==null && config.autoCreateQueues ) {
        addQueue(destination)(cb)
      } else  {
        cb(queue)
      }
    }
  } |>>: dispatchQueue


  def addQueue(dest:Destination)(cb: (Queue)=>Unit ) = ^{
    val name = DestinationParser.toBuffer(dest, destination_parser_options)
    if( store!=null ) {
      val record = new QueueRecord
      record.name = name
      store.addQueue(record) { rc =>
        rc match {
          case Some(id) =>
            dispatchQueue ^ {
              val queue = new Queue(this, dest, id)
              queues.put(name, queue)
              cb(queue)
            }
          case None => // store could not create
            cb(null)
        }
      }
    } else {
      val queue = new Queue(this, dest, -1)
      queues.put(name, queue)
      cb(queue)
    }

  } |>>: dispatchQueue

  def createSubscription(consumer:ConsumerContext):BrokerSubscription = {
      createSubscription(consumer, consumer.getDestination());
  }

  def createSubscription(consumer:ConsumerContext, destination:Destination):BrokerSubscription = {

      // First handle composite destinations..
      var destinations = destination.getDestinations();
      if (destinations != null) {
          var subs :List[BrokerSubscription] = Nil
          for (childDest <- destinations) {
              subs ::= createSubscription(consumer, childDest);
          }
          return new CompositeSubscription(destination, subs);
      }

      // If it's a Topic...
//      if ( destination.getDomain == TOPIC_DOMAIN || destination.getDomain == TEMP_TOPIC_DOMAIN ) {
//
//          // It might be a durable subscription on the topic
//          if (consumer.isDurable()) {
//              var dsub = durableSubs.get(consumer.getSubscriptionName());
//              if (dsub == null) {
////                    TODO:
////                    IQueue<Long, MessageDelivery> queue = queueStore.createDurableQueue(consumer.getSubscriptionName());
////                    queue.start();
////                    dsub = new DurableSubscription(this, destination, consumer.getSelectorExpression(), queue);
////                    durableSubs.put(consumer.getSubscriptionName(), dsub);
//              }
//              return dsub;
//          }
//
//          // return a standard subscription
////            TODO:
////            return new TopicSubscription(this, destination, consumer.getSelectorExpression());
//          return null;
//      }

      // It looks like a wild card subscription on a queue..
      if (PathFilter.containsWildCards(destination.getName())) {
          return new WildcardQueueSubscription(this, destination, consumer);
      }

      // It has to be a Queue subscription then..
      var queue = queues.get(destination.getName());
      if (queue == null) {
          if (consumer.autoCreateDestination()) {
//            TODO
//              queue = createQueue(destination);
          } else {
              throw new IllegalStateException("The queue does not exist: " + destination.getName());
          }
      }
//        TODO:
//        return new Queue.QueueSubscription(queue);
      return null;
  }


  val queueLifecyleListeners = new ArrayList[QueueLifecyleListener]();

  def addDestinationLifecyleListener(listener:QueueLifecyleListener):Unit= {
      queueLifecyleListeners.add(listener);
  }

  def removeDestinationLifecyleListener(listener:QueueLifecyleListener):Unit= {
      queueLifecyleListeners.add(listener);
  }
}

///**
// * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
// */
//class BrokerDatabase() {
//
//  @BeanProperty
//  var store:Store=new MemoryStore;
//
//  @BeanProperty
//  var virtualHost:VirtualHost=null;
//
//    def start() ={
//        //To change body of implemented methods use File | Settings | File Templates.
//    }
//
//    def stop() = {
//        //To change body of implemented methods use File | Settings | File Templates.
//    }
//
// TODO: re-implement.
//    private static final boolean DEBUG = false;
//
//    private final Flow databaseFlow = new Flow("database", false);
//
//    private final SizeLimiter<OperationBase<?>> storeLimiter;
//    private final FlowController<OperationBase<?>> storeController;
//    private final int FLUSH_QUEUE_SIZE = 10000 * 1024;
//
//    private DispatchQueue dispatcher;
//    private Thread flushThread;
//    private AtomicBoolean running = new AtomicBoolean(false);
//    private DatabaseListener listener;
//
//    private final LinkedNodeList<OperationBase<?>> opQueue;
//    private AtomicBoolean notify = new AtomicBoolean(false);
//    private Semaphore opsReady = new Semaphore(0);
//    private long opSequenceNumber;
//    private long flushPointer = -1; // The last seq num for which flush was
//    // requested
//    private long requestedDelayedFlushPointer = -1; // Set to the last sequence
//    // num scheduled for delay
//    private long delayedFlushPointer = 0; // The last delayable sequence num
//    // requested.
//    private long flushDelay = 10;
//
//    private final Runnable flushDelayCallback;
//    private boolean storeBypass = true;
//
//    public interface DatabaseListener {
//        /**
//         * Called if there is a catastrophic problem with the database.
//         *
//         * @param ioe
//         *            The causing exception.
//         */
//        public void onDatabaseException(IOException ioe);
//    }
//
//    public static interface MessageRecordMarshaller<V> {
//        MessageRecord marshal(V element);
//
//        /**
//         * Called when a queue element is recovered from the store for a
//         * particular queue.
//         *
//         * @param mRecord
//         *            The message record
//         * @param queue
//         *            The queue that the element is being restored to (or null
//         *            if not being restored for a queue)
//         * @return
//         */
//        V unMarshall(MessageRecord mRecord, QueueDescriptor queue);
//    }
//
//    public BrokerDatabase(Store store) {
//        this.store = store;
//        this.opQueue = new LinkedNodeList<OperationBase<?>>();
//        storeLimiter = new SizeLimiter<OperationBase<?>>(FLUSH_QUEUE_SIZE, 0) {
//
//            @Override
//            public int getElementSize(OperationBase<?> op) {
//                return op.getLimiterSize();
//            }
//        };
//
//        storeController = new FlowController<OperationBase<?>>(new FlowControllable<OperationBase<?>>() {
//
//            public void flowElemAccepted(ISourceController<OperationBase<?>> controller, OperationBase<?> op) {
//                addToOpQueue(op);
//            }
//
//            public IFlowResource getFlowResource() {
//                return BrokerDatabase.this;
//            }
//
//        }, databaseFlow, storeLimiter, opQueue);
//        storeController.useOverFlowQueue(false);
//        super.onFlowOpened(storeController);
//
//        flushDelayCallback = new Runnable() {
//            public void run() {
//                flushDelayCallback();
//            }
//        };
//    }
//
//    public synchronized void start() throws Exception {
//        if (flushThread == null) {
//
//            running.set(true);
//            store.start();
//            flushThread = new Thread(new Runnable() {
//
//                public void run() {
//                    processOps();
//                }
//
//            }, "StoreThread");
//            flushThread.start();
//        }
//    }
//
//    public synchronized void stop() throws Exception {
//        if (flushThread != null) {
//
//            synchronized (opQueue) {
//                updateFlushPointer(opSequenceNumber + 1);
//            }
//
//            running.set(false);
//            boolean interrupted = false;
//            while (true) {
//                opsReady.release();
//                try {
//                    flushThread.join();
//                    break;
//                } catch (InterruptedException e) {
//                    interrupted = true;
//                }
//            }
//
//            store.flush();
//            store.stop();
//
//            if (interrupted) {
//                Thread.currentThread().interrupt();
//            }
//            flushThread = null;
//        }
//    }
//
//    /**
//     * A blocking operation that lists all queues of a given type:
//     *
//     * @param type
//     *            The queue type
//     * @return A list of queues.
//     *
//     * @throws Exception
//     *             If there was an error listing the queues.
//     */
//    public Iterator<QueueQueryResult> listQueues(final short type) throws Exception {
//        return store.execute(new Callback<Iterator<QueueQueryResult>, Exception>() {
//
//            public Iterator<QueueQueryResult> execute(Session session) throws Exception {
//                return session.queueListByType(type, null, Integer.MAX_VALUE);
//            }
//
//        }, null);
//    }
//
//    /**
//     * A blocking operation that lists all entries in the specified map
//     *
//     * @param map
//     *            The map to list
//     * @return A list of map entries
//     *
//     * @throws Exception
//     *             If there was an error listing the queues.
//     */
//    public Map<AsciiBuffer, Buffer> listMapEntries(final AsciiBuffer map) throws Exception {
//        return store.execute(new Callback<Map<AsciiBuffer, Buffer>, Exception>() {
//
//            public Map<AsciiBuffer, Buffer> execute(Session session) throws Exception {
//                HashMap<AsciiBuffer, Buffer> ret = new HashMap<AsciiBuffer, Buffer>();
//                try {
//                    Iterator<AsciiBuffer> keys = session.mapEntryListKeys(map, null, -1);
//                    while (keys.hasNext()) {
//                        AsciiBuffer key = keys.next();
//                        ret.put(key, session.mapEntryGet(map, key));
//                    }
//                } catch (Store.KeyNotFoundException knfe) {
//                    //No keys then:
//                }
//
//                return ret;
//            }
//
//        }, null);
//    }
//
//    /**
//     * @param map
//     *            The name of the map to update.
//     * @param key
//     *            The key in the map to update.
//     * @param value
//     *            The value to insert.
//     */
//    public OperationContext<?> updateMapEntry(AsciiBuffer map, AsciiBuffer key, Buffer value) {
//        return add(new MapUpdateOperation(map, key, value), null, false);
//    }
//
//    /**
//     * Executes user supplied {@link Operation}. If the {@link Operation} does
//     * not throw any Exceptions, all updates to the store are committed,
//     * otherwise they are rolled back. Any exceptions thrown by the
//     * {@link Operation} are propagated by this method.
//     *
//     * If limiter space on the store processing queue is exceeded, the
//     * controller will be blocked.
//     *
//     * If this method is called with flush set to
//     * <code>false</false> there is no
//     * guarantee made about when the operation will be executed. If <code>flush</code>
//     * is <code>true</code> and {@link Operation#isDelayable()} is also
//     * <code>true</code> then an attempt will be made to execute the event at
//     * the {@link Store}'s configured delay interval.
//     *
//     * @param op
//     *            The operation to execute
//     * @param flush
//     *            Whether or not this operation needs immediate processing.
//     * @param controller
//     *            the source of the operation.
//     * @return the {@link OperationContext} associated with the operation
//     */
//    private <T> OperationContext<T> add(OperationBase<T> op, ISourceController<?> controller, boolean flush) {
//
//        op.flushRequested = flush;
//        storeController.add(op, controller);
//        return op;
//    }
//
//    private final void addToOpQueue(OperationBase<?> op) {
//        if (!running.get()) {
//            throw new IllegalStateException("BrokerDatabase not started");
//        }
//
//        synchronized (opQueue) {
//            op.opSequenceNumber = opSequenceNumber++;
//            opQueue.addLast(op);
//            if (op.flushRequested || storeLimiter.getThrottled()) {
//                if (op.isDelayable() && flushDelay > 0) {
//                    scheduleDelayedFlush(op.opSequenceNumber);
//                } else {
//                    updateFlushPointer(op.opSequenceNumber);
//                }
//            }
//        }
//    }
//
//    private void updateFlushPointer(long seqNumber) {
//        if (seqNumber > flushPointer) {
//            flushPointer = seqNumber;
//            OperationBase<?> op = opQueue.getHead();
//            if (op != null && op.opSequenceNumber <= flushPointer && notify.get()) {
//                opsReady.release();
//            }
//        }
//    }
//
//    private void scheduleDelayedFlush(long seqNumber) {
//        if (seqNumber < flushPointer) {
//            return;
//        }
//
//        if (seqNumber > delayedFlushPointer) {
//            delayedFlushPointer = seqNumber;
//        }
//
//        if (requestedDelayedFlushPointer == -1) {
//            requestedDelayedFlushPointer = delayedFlushPointer;
//            Dispatch.getGlobalQueue().dispatchAfter(flushDelay, TimeUnit.MILLISECONDS, flushDelayCallback);
//        }
//
//    }
//
//    private final void flushDelayCallback() {
//        synchronized (opQueue) {
//            if (flushPointer < requestedDelayedFlushPointer) {
//                updateFlushPointer(requestedDelayedFlushPointer);
//
//            }
//
//            // If another delayed flush has been scheduled schedule it:
//            requestedDelayedFlushPointer = -1;
//            // Schedule next delay if needed:
//            if (delayedFlushPointer > flushPointer) {
//                scheduleDelayedFlush(delayedFlushPointer);
//            } else {
//                delayedFlushPointer = -1;
//            }
//
//        }
//    }
//
//    private final OperationBase<?> getNextOp(boolean wait) {
//        if (!wait) {
//            synchronized (opQueue) {
//                OperationBase<?> op = opQueue.getHead();
//                if (op != null && (op.opSequenceNumber <= flushPointer || !op.isDelayable())) {
//                    op.unlink();
//                    return op;
//                }
//            }
//            return null;
//        } else {
//            OperationBase<?> op = getNextOp(false);
//            if (op == null) {
//                notify.set(true);
//                op = getNextOp(false);
//                try {
//                    while (running.get() && op == null) {
//                        opsReady.acquireUninterruptibly();
//                        op = getNextOp(false);
//                    }
//                } finally {
//                    notify.set(false);
//                    opsReady.drainPermits();
//                }
//            }
//            return op;
//        }
//    }
//
//    private final void processOps() {
//        int count = 0;
//        Session session = store.getSession();
//        while (running.get()) {
//            final OperationBase<?> firstOp = getNextOp(true);
//            if (firstOp == null) {
//                continue;
//            }
//            count = 0;
//
//            // The first operation we get, triggers a store transaction.
//            if (firstOp != null) {
//                final LinkedList<Operation<?>> processedQueue = new LinkedList<Operation<?>>();
//                boolean locked = false;
//                try {
//
//                    Operation<?> op = firstOp;
//                    while (op != null) {
//                        final Operation<?> toExec = op;
//                        if (toExec.beginExecute()) {
//                            if (!locked) {
//                                session.acquireLock();
//                                locked = true;
//                            }
//                            count++;
//                            op.execute(session);
//                            processedQueue.add(op);
//                            /*
//                             * store.execute(new Store.VoidCallback<Exception>()
//                             * {
//                             *
//                             * @Override public void run(Session session) throws
//                             * Exception {
//                             *
//                             * // Try to execute the operation against the //
//                             * session... try { toExec.execute(session);
//                             * processedQueue.add(toExec); } catch
//                             * (CancellationException ignore) { //
//                             * System.out.println("Cancelled" + // toExec); } }
//                             * }, null);
//                             */
//                        }
//
//                        if (count < 1000) {
//                            op = getNextOp(false);
//                        } else {
//                            op = null;
//                        }
//                    }
//                    // executeOps(firstOp, processedQueue, counter);
//
//                    // If we procecessed some ops, flush and post process:
//                    if (!processedQueue.isEmpty()) {
//
//                        if (locked) {
//                            session.commit();
//                            session.releaseLock();
//                            locked = false;
//                        }
//                        if (DEBUG)
//                            System.out.println("Flushing queue after processing: " + processedQueue.size() + " - " + processedQueue);
//                        // Sync the store:
//                        store.flush();
//
//                        // Post process operations
//                        long release = 0;
//                        for (Operation<?> processed : processedQueue) {
//                            processed.onCommit();
//                            // System.out.println("Processed" + processed);
//                            release += processed.getLimiterSize();
//                        }
//
//                        synchronized (opQueue) {
//                            this.storeLimiter.remove(1, release);
//                        }
//                    }
//
//                } catch (IOException e) {
//                    for (Operation<?> processed : processedQueue) {
//                        processed.onRollback(e);
//                    }
//                    onDatabaseException(e);
//                } catch (RuntimeException e) {
//                    for (Operation<?> processed : processedQueue) {
//                        processed.onRollback(e);
//                    }
//                    IOException ioe = new IOException(e.getMessage());
//                    ioe.initCause(e);
//                    onDatabaseException(ioe);
//                } catch (Exception e) {
//                    for (Operation<?> processed : processedQueue) {
//                        processed.onRollback(e);
//                    }
//                    IOException ioe = new IOException(e.getMessage());
//                    ioe.initCause(e);
//                    onDatabaseException(ioe);
//                } finally {
//                    if (locked) {
//                        try {
//                            session.releaseLock();
//                        } catch (Exception e) {
//                            IOException ioe = new IOException(e.getMessage());
//                            ioe.initCause(e);
//                            onDatabaseException(ioe);
//                        }
//                    }
//                }
//            }
//        }
//    }
//
//    /*
//     * private final void executeOps(final OperationBase op, final
//     * LinkedList<Operation> processedQueue, final OpCounter counter) throws
//     * FatalStoreException, Exception { store.execute(new
//     * Store.VoidCallback<Exception>() {
//     *
//     * @Override public void run(Session session) throws Exception {
//     *
//     * // Try to execute the operation against the // session... try { if
//     * (op.execute(session)) { processedQueue.add(op); } else { counter.count--;
//     * } } catch (CancellationException ignore) { System.out.println("Cancelled"
//     * + op); }
//     *
//     * // See if we can batch up some additional operations // in this
//     * transaction. if (counter.count < 100) { OperationBase next =
//     * getNextOp(false); if (next != null) { counter.count++; executeOps(next,
//     * processedQueue, counter); } } } }, null); }
//     */
//
//    /**
//     * Adds a queue to the database
//     *
//     * @param queue
//     *            The queue to add.
//     */
//    public void addQueue(QueueDescriptor queue) {
//        add(new QueueAddOperation(queue), null, false);
//    }
//
//    /**
//     * Deletes a queue and all of its messages from the database
//     *
//     * @param queue
//     *            The queue to delete.
//     */
//    public void deleteQueue(QueueDescriptor queue) {
//        add(new QueueDeleteOperation(queue), null, false);
//    }
//
//    /**
//     * Saves a message for all of the recipients in the
//     * {@link BrokerMessageDelivery}.
//     *
//     * @param delivery
//     *            The delivery.
//     * @param source
//     *            The source's controller.
//     * @throws IOException
//     *             If there is an error marshalling the message.
//     * @return The {@link OperationContext} associated with the operation
//     */
//    public OperationContext<?> persistReceivedMessage(BrokerMessageDelivery delivery, ISourceController<?> source) {
//        return add(new AddMessageOperation(delivery), source, true);
//    }
//
//    /**
//     * Saves a Message for a single queue.
//     *
//     * @param queueElement
//     *            The element to save.
//     * @param source
//     *            The source initiating the save or null, if there isn't one.
//     * @throws IOException
//     *             If there is an error marshalling the message.
//     *
//     * @return The {@link OperationContext} associated with the operation
//     */
//    public OperationContext<?> saveMessage(SaveableQueueElement<MessageDelivery> queueElement, ISourceController<?> source, boolean delayable) {
//        return add(new AddMessageOperation(queueElement), source, !delayable);
//    }
//
//    /**
//     * Deletes the given message from the store for the given queue.
//     *
//     * @param queueElement
//     * @return The {@link OperationContext} associated with the operation
//     */
//    public OperationContext<?> deleteQueueElement(SaveableQueueElement<?> queueElement) {
//        return add(new DeleteOperation(queueElement.getSequenceNumber(), queueElement.getQueueDescriptor()), null, false);
//    }
//
//    /**
//     * Loads a batch of messages for the specified queue. The loaded messages
//     * are given the provided {@link RestoreListener}.
//     * <p>
//     * <b><i>NOTE:</i></b> This method uses the queue sequence number for the
//     * message not the store tracking number.
//     *
//     * @param queue
//     *            The queue for which to load messages
//     * @param recordsOnly
//     *            True if message body shouldn't be restored
//     * @param first
//     *            The first queue sequence number to load (-1 starts at
//     *            begining)
//     * @param maxSequence
//     *            The maximum sequence number to load (-1 if no limit)
//     * @param maxCount
//     *            The maximum number of messages to load (-1 if no limit)
//     * @param listener
//     *            The listener to which messags should be passed.
//     * @return The {@link OperationContext} associated with the operation
//     */
//    public <T> OperationContext<?> restoreQueueElements(QueueDescriptor queue, boolean recordsOnly, long first, long maxSequence, int maxCount, RestoreListener<T> listener,
//            MessageRecordMarshaller<T> marshaller) {
//        return add(new RestoreElementsOperation<T>(queue, recordsOnly, first, maxCount, maxSequence, listener, marshaller), null, true);
//    }
//
//    private void onDatabaseException(IOException ioe) {
//        if (listener != null) {
//            listener.onDatabaseException(ioe);
//        } else {
//            ioe.printStackTrace();
//        }
//    }
//
//    public interface OperationContext<V> extends ListenableFuture<V> {
//
//        /**
//         * Attempts to cancel the store operation. Returns true if the operation
//         * could be canceled or false if the operation was already executed by
//         * the store.
//         *
//         * @return true if the operation could be canceled
//         */
//        public boolean cancel();
//
//        /**
//         * Requests flush for this database operation (overriding a previous
//         * delay)
//         */
//        public void requestFlush();
//    }
//
//    /**
//     * This interface is used to execute transacted code.
//     *
//     * It is used by the {@link Store#execute(org.apache.activemq.broker.store.Store.Callback, Runnable)} method, often as
//     * anonymous class.
//     */
//    public interface Operation<V> extends OperationContext<V> {
//
//        /**
//         * Called when the saver is about to execute the operation. If true is
//         * returned the operation can no longer be canceled.
//         *
//         * @return false if the operation has been canceled.
//         */
//        public boolean beginExecute();
//
//        /**
//         * Gets called by the {@link Store}
//         * within a transactional context. If any exception is thrown including
//         * Runtime exception, the transaction is rolled back.
//         *
//         * @param session
//         *            provides you access to read and update the persistent
//         *            data.
//         * @throws Exception
//         *             if an system error occured while executing the
//         *             operations.
//         * @throws RuntimeException
//         *             if an system error occured while executing the
//         *             operations.
//         */
//        public void execute(Session session) throws CancellationException, Exception, RuntimeException;
//
//        /**
//         * Returns true if this operation can be delayed. This is useful in
//         * cases where external events can negate the need to execute the
//         * operation. The delay interval is not guaranteed to be honored, if
//         * subsequent events or other store flush policy/criteria requires a
//         * flush of subsequent events.
//         *
//         * @return True if the operation can be delayed.
//         */
//        public boolean isDelayable();
//
//        /**
//         * Returns the size to be used when calculating how much space this
//         * operation takes on the store processing queue.
//         *
//         * @return The limiter size to be used.
//         */
//        public int getLimiterSize();
//
//        /**
//         * Called after {@link #execute(Session)} is called and the the
//         * operation has been committed.
//         */
//        public void onCommit();
//
//        /**
//         * Called after {@link #execute(Session)} is called and the the
//         * operation has been rolled back.
//         */
//        public void onRollback(Throwable error);
//    }
//
//    /**
//     * This is a convenience base class that can be used to implement
//     * Operations. It handles operation cancellation for you.
//     */
//    abstract class OperationBase<V> extends LinkedNode<OperationBase<?>> implements Operation<V> {
//        public boolean flushRequested = false;
//        public long opSequenceNumber = -1;
//
//        final protected AtomicBoolean executePending = new AtomicBoolean(true);
//        final protected AtomicBoolean cancelled = new AtomicBoolean(false);
//        final protected AtomicBoolean executed = new AtomicBoolean(false);
//        final protected AtomicReference<FutureListener<? super V>> listener = new AtomicReference<FutureListener<? super V>>();
//
//        protected Throwable error;
//
//        public static final int BASE_MEM_SIZE = 20;
//
//        public boolean cancel(boolean interrupt) {
//            return cancel();
//        }
//
//        public boolean cancel() {
//            if (storeBypass) {
//                if (executePending.compareAndSet(true, false)) {
//                    cancelled.set(true);
//                    // System.out.println("Cancelled: " + this);
//                    synchronized (opQueue) {
//                        unlink();
//                        storeController.elementDispatched(this);
//                    }
//                    fireListener();
//                    return true;
//                }
//            }
//            return cancelled.get();
//        }
//
//        public final boolean isCancelled() {
//            return cancelled.get();
//        }
//
//        public final boolean isExecuted() {
//            return executed.get();
//        }
//
//        public final boolean isDone() {
//            return isCancelled() || isExecuted();
//        }
//
//        /**
//         * Called when the saver is about to execute the operation. If true is
//         * returned the operation can no longer be cancelled.
//         *
//         * @return true if operation should be executed
//         */
//        public final boolean beginExecute() {
//            if (executePending.compareAndSet(true, false)) {
//                return true;
//            } else {
//                return false;
//            }
//        }
//
//        /**
//         * Gets called by the
//         * {@link Store} method
//         * within a transactional context. If any exception is thrown including
//         * Runtime exception, the transaction is rolled back.
//         *
//         * @param session
//         *            provides you access to read and update the persistent
//         *            data.
//         * @throws Exception
//         *             if an system error occured while executing the
//         *             operations.
//         * @throws RuntimeException
//         *             if an system error occured while executing the
//         *             operations.
//         */
//        public void execute(Session session) throws Exception, RuntimeException {
//            if (DEBUG)
//                System.out.println("Executing " + this);
//            doExcecute(session);
//        }
//
//        abstract protected void doExcecute(Session session);
//
//        public int getLimiterSize() {
//            return BASE_MEM_SIZE;
//        }
//
//        public boolean isDelayable() {
//            return false;
//        }
//
//        /**
//         * Requests flush for this database operation (overriding a previous
//         * delay)
//         */
//        public void requestFlush() {
//            synchronized (opQueue) {
//                updateFlushPointer(opSequenceNumber);
//            }
//        }
//
//        public void onCommit() {
//            executed.set(true);
//            fireListener();
//        }
//
//        /**
//         * Called after {@link #execute(Session)} is called and the the
//         * operation has been rolled back.
//         */
//        public void onRollback(Throwable error) {
//            executed.set(true);
//            if (!fireListener()) {
//                error.printStackTrace();
//            }
//        }
//
//        private final boolean fireListener() {
//            FutureListener<? super V> l = this.listener.getAndSet(null);
//            if (l != null) {
//                l.onFutureComplete(this);
//                return true;
//            }
//            return false;
//        }
//
//        public void setFutureListener(FutureListener<? super V> listener) {
//            this.listener.set(listener);
//            if (isDone()) {
//                fireListener();
//            }
//        }
//
//        /**
//         * Subclasses the return a result should override this
//         * @return The result.
//         */
//        protected final V getResult() {
//            return null;
//        }
//
//        /**
//         * Waits if necessary for the computation to complete, and then
//         * retrieves its result.
//         *
//         * @return the computed result
//         * @throws CancellationException
//         *             if the computation was cancelled
//         * @throws ExecutionException
//         *             if the computation threw an exception
//         * @throws InterruptedException
//         *             if the current thread was interrupted while waiting
//         */
//        public final V get() throws ExecutionException, InterruptedException  {
//
//            try {
//                return get(-1, TimeUnit.MILLISECONDS);
//            } catch (TimeoutException e) {
//                //Can't happen.
//                throw new AssertionError(e);
//            }
//        }
//
//        /**
//         * Waits if necessary for at most the given time for the computation
//         * to complete, and then retrieves its result, if available.
//         *
//         * @param timeout the maximum time to wait
//         * @param tu the time unit of the timeout argument
//         * @return the computed result
//         * @throws CancellationException if the computation was cancelled
//         * @throws ExecutionException if the computation threw an
//         * exception
//         * @throws InterruptedException if the current thread was interrupted
//         * while waiting
//         * @throws TimeoutException if the wait timed out
//         */
//        public final V get(long timeout, TimeUnit tu) throws ExecutionException, InterruptedException, TimeoutException {
//            if (isCancelled()) {
//                throw new CancellationException();
//            }
//            if (error != null) {
//                throw new ExecutionException(error);
//            }
//
//            //TODO implement blocking?
//            if(!isDone())
//            {
//                throw new UnsupportedOperationException("Blocking result retrieval not yet implemented");
//            }
//
//            return getResult();
//        }
//
//        public String toString() {
//            return "DBOp seq: " + opSequenceNumber + "P/C/E: " + executePending.get() + "/" + isCancelled() + "/" + isExecuted();
//        }
//    }
//
//    private class QueueAddOperation extends OperationBase<Object> {
//
//        private QueueDescriptor qd;
//
//        QueueAddOperation(QueueDescriptor queue) {
//            qd = queue;
//        }
//
//        @Override
//        protected void doExcecute(Session session) {
//            try {
//                session.queueAdd(qd);
//            } catch (KeyNotFoundException e) {
//                throw new FatalStoreException(e);
//            }
//        }
//
//        public String toString() {
//            return "QueueAdd: " + qd.getQueueName().toString();
//        }
//    }
//
//    private class QueueDeleteOperation extends OperationBase<Object> {
//
//        private QueueDescriptor qd;
//
//        QueueDeleteOperation(QueueDescriptor queue) {
//            qd = queue;
//        }
//
//        @Override
//        protected void doExcecute(Session session) {
//            session.queueRemove(qd);
//        }
//
//        public String toString() {
//            return "QueueDelete: " + qd.getQueueName().toString();
//        }
//    }
//
//    private class DeleteOperation extends OperationBase<Object> {
//        private final long queueKey;
//        private QueueDescriptor queue;
//
//        public DeleteOperation(long queueKey, QueueDescriptor queue) {
//            this.queueKey = queueKey;
//            this.queue = queue;
//        }
//
//        @Override
//        public int getLimiterSize() {
//            return BASE_MEM_SIZE + 8;
//        }
//
//        @Override
//        protected void doExcecute(Session session) {
//            try {
//                session.queueRemoveMessage(queue, queueKey);
//            } catch (KeyNotFoundException e) {
//                // TODO Probably doesn't always mean an error, it is possible
//                // that
//                // the queue has been deleted, in which case its messages will
//                // have been deleted, too.
//                e.printStackTrace();
//            }
//        }
//
//        public String toString() {
//            return "MessageDelete: " + queue.getQueueName().toString() + " tracking: " + queueKey + " " + super.toString();
//        }
//    }
//
//    private class RestoreElementsOperation<V> extends OperationBase<V> {
//        private QueueDescriptor queue;
//        private long firstKey;
//        private int maxRecords;
//        private long maxSequence;
//        private boolean recordsOnly;
//        private RestoreListener<V> listener;
//        private Collection<RestoredElement<V>> msgs = null;
//        private MessageRecordMarshaller<V> marshaller;
//
//        RestoreElementsOperation(QueueDescriptor queue, boolean recordsOnly, long firstKey, int maxRecords, long maxSequence, RestoreListener<V> listener, MessageRecordMarshaller<V> marshaller) {
//            this.queue = queue;
//            this.recordsOnly = recordsOnly;
//            this.firstKey = firstKey;
//            this.maxRecords = maxRecords;
//            this.maxSequence = maxSequence;
//            this.listener = listener;
//            this.marshaller = marshaller;
//        }
//
//        @Override
//        public int getLimiterSize() {
//            return BASE_MEM_SIZE + 44;
//        }
//
//        @Override
//        protected void doExcecute(Session session) {
//
//            Iterator<QueueRecord> records = null;
//            try {
//                records = session.queueListMessagesQueue(queue, firstKey, maxSequence, maxRecords);
//                msgs = new LinkedList<RestoredElement<V>>();
//            } catch (KeyNotFoundException e) {
//                msgs = new ArrayList<RestoredElement<V>>(0);
//                return;
//            }
//
//            QueueRecord qRecord = null;
//            int count = 0;
//            if (records.hasNext()) {
//                qRecord = records.next();
//            }
//
//            while (qRecord != null) {
//                RestoredElementImpl<V> rm = new RestoredElementImpl<V>();
//                // TODO should update jms redelivery here.
//                rm.qRecord = qRecord;
//                rm.queue = queue;
//                count++;
//
//                // Set the next sequence number:
//                if (records.hasNext()) {
//                    qRecord = records.next();
//                    rm.nextSequence = qRecord.getQueueKey();
//                } else {
//                    // Look up the next sequence number:
//                    try {
//                        records = session.queueListMessagesQueue(queue, qRecord.getQueueKey() + 1, -1L, 1);
//                        if (!records.hasNext()) {
//                            rm.nextSequence = -1;
//                        } else {
//                            rm.nextSequence = records.next().getQueueKey();
//                        }
//                    } catch (KeyNotFoundException e) {
//                        rm.nextSequence = -1;
//                    }
//                    qRecord = null;
//                }
//
//                if (!recordsOnly) {
//                    try {
//                        rm.mRecord = session.messageGetRecord(rm.qRecord.getMessageKey());
//                        rm.marshaller = marshaller;
//                        msgs.add(rm);
//                    } catch (KeyNotFoundException shouldNotHappen) {
//                        shouldNotHappen.printStackTrace();
//                    }
//                } else {
//                    msgs.add(rm);
//                }
//            }
//
//            if (DEBUG)
//                System.out.println("Restored: " + count + " messages");
//        }
//
//        @Override
//        public void onCommit() {
//            listener.elementsRestored(msgs);
//            super.onCommit();
//        }
//
//        public String toString() {
//            return "MessageRestore: " + queue.getQueueName().toString() + " first: " + firstKey + " max: " + maxRecords;
//        }
//    }
//
//    private class AddMessageOperation extends OperationBase<Object> {
//
//        private final BrokerMessageDelivery brokerDelivery;
//        private final SaveableQueueElement<MessageDelivery> singleElement;
//        private final MessageDelivery delivery;
//        private MessageRecord record;
//        private LinkedList<SaveableQueueElement<MessageDelivery>> notifyTargets;
//        private final boolean delayable;
//
//        public AddMessageOperation(BrokerMessageDelivery delivery) {
//            this.brokerDelivery = delivery;
//            this.singleElement = null;
//            this.delivery = delivery;
//            this.delayable = delivery.isFlushDelayable();
//            if (!delayable) {
//                this.record = delivery.createMessageRecord();
//            }
//        }
//
//        public AddMessageOperation(SaveableQueueElement<MessageDelivery> queueElement) {
//            this.brokerDelivery = null;
//            singleElement = queueElement;
//            delivery = queueElement.getElement();
//            this.record = singleElement.getElement().createMessageRecord();
//            delayable = false;
//        }
//
//        public boolean isDelayable() {
//            return delayable;
//        }
//
//        @Override
//        public int getLimiterSize() {
//            return delivery.getFlowLimiterSize() + BASE_MEM_SIZE + 40;
//        }
//
//        @Override
//        protected void doExcecute(Session session) {
//
//            if (singleElement == null) {
//                brokerDelivery.beginStore();
//                Collection<SaveableQueueElement<MessageDelivery>> targets = brokerDelivery.getPersistentQueues();
//
//                if (targets != null && !targets.isEmpty()) {
//                    if (record == null) {
//                        record = brokerDelivery.createMessageRecord();
//                        if (record == null) {
//                            throw new RuntimeException("Error creating message record for " + brokerDelivery.getMsgId());
//                        }
//                    }
//                    record.setId(brokerDelivery.getStoreTracking());
//                    session.messageAdd(record);
//
//                    for (SaveableQueueElement<MessageDelivery> target : targets) {
//                        try {
//                            QueueRecord queueRecord = new QueueRecord();
//                            queueRecord.setAttachment(null);
//                            queueRecord.setMessageKey(record.getId());
//                            queueRecord.setSize(brokerDelivery.getFlowLimiterSize());
//                            queueRecord.setQueueKey(target.getSequenceNumber());
//                            session.queueAddMessage(target.getQueueDescriptor(), queueRecord);
//
//                        } catch (KeyNotFoundException e) {
//                            e.printStackTrace();
//                        }
//
//                        if (target.requestSaveNotify()) {
//                            if (notifyTargets == null) {
//                                notifyTargets = new LinkedList<SaveableQueueElement<MessageDelivery>>();
//                            }
//                            notifyTargets.add(target);
//                        }
//                    }
//                } else {
//                    // Save with no targets must have been cancelled:
//                    // System.out.println("Skipping save for " +
//                    // delivery.getStoreTracking());
//                }
//            } else {
//
//                session.messageAdd(record);
//                try {
//                    QueueRecord queueRecord = new QueueRecord();
//                    queueRecord.setAttachment(null);
//                    queueRecord.setMessageKey(record.getId());
//                    queueRecord.setSize(brokerDelivery.getFlowLimiterSize());
//                    queueRecord.setQueueKey(singleElement.getSequenceNumber());
//                    session.queueAddMessage(singleElement.getQueueDescriptor(), queueRecord);
//                } catch (KeyNotFoundException e) {
//                    e.printStackTrace();
//                }
//            }
//        }
//
//        @Override
//        public void onCommit() {
//
//            // Notify that the message was persisted.
//            delivery.onMessagePersisted();
//
//            // Notify any of the targets that requested notify on save:
//            if (singleElement != null && singleElement.requestSaveNotify()) {
//                singleElement.notifySave();
//            } else if (notifyTargets != null) {
//                for (SaveableQueueElement<MessageDelivery> notify : notifyTargets) {
//                    notify.notifySave();
//                }
//            }
//
//            super.onCommit();
//        }
//
//        public String toString() {
//            return "AddOperation " + delivery.getStoreTracking() + super.toString();
//        }
//    }
//
//    private class MapUpdateOperation extends OperationBase<Object> {
//        final AsciiBuffer map;
//        final AsciiBuffer key;
//        final Buffer value;
//
//        MapUpdateOperation(AsciiBuffer mapName, AsciiBuffer key, Buffer value) {
//            this.map = mapName;
//            this.key = key;
//            this.value = value;
//        }
//
//        @Override
//        public int getLimiterSize() {
//            return BASE_MEM_SIZE + map.length + key.length + value.length;
//        }
//
//        /*
//         * (non-Javadoc)
//         *
//         * @see
//         * org.apache.activemq.apollo.broker.BrokerDatabase.OperationBase#doExcecute
//         * (org.apache.activemq.broker.store.Store.Session)
//         */
//        @Override
//        protected void doExcecute(Session session) {
//            try {
//                session.mapEntryPut(map, key, value);
//            } catch (KeyNotFoundException e) {
//                throw new Store.FatalStoreException(e);
//            }
//        }
//    }
//
//    private class RestoredElementImpl<T> implements RestoredElement<T> {
//        QueueRecord qRecord;
//        QueueDescriptor queue;
//        MessageRecord mRecord;
//        MessageRecordMarshaller<T> marshaller;
//        long nextSequence;
//
//        public T getElement() throws IOException {
//            if (mRecord == null) {
//                return null;
//            }
//            return marshaller.unMarshall(mRecord, queue);
//        }
//
//        /*
//         * (non-Javadoc)
//         *
//         * @see
//         * org.apache.activemq.queue.QueueStore.RestoredElement#getSequenceNumber
//         * ()
//         */
//        public long getSequenceNumber() {
//            return qRecord.getQueueKey();
//        }
//
//        /*
//         * (non-Javadoc)
//         *
//         * @see
//         * org.apache.activemq.queue.QueueStore.RestoredElement#getStoreTracking
//         * ()
//         */
//        public long getStoreTracking() {
//            return qRecord.getMessageKey();
//        }
//
//        /*
//         * (non-Javadoc)
//         *
//         * @seeorg.apache.activemq.queue.QueueStore.RestoredElement#
//         * getNextSequenceNumber()
//         */
//        public long getNextSequenceNumber() {
//            return nextSequence;
//        }
//
//        /*
//         * (non-Javadoc)
//         *
//         * @see
//         * org.apache.activemq.queue.QueueStore.RestoredElement#getElementSize()
//         */
//        public int getElementSize() {
//            return qRecord.getSize();
//        }
//
//        /*
//         * (non-Javadoc)
//         *
//         * @see
//         * org.apache.activemq.queue.QueueStore.RestoredElement#getExpiration()
//         */
//        public long getExpiration() {
//            return qRecord.getTte();
//        }
//    }
//
//    public long allocateStoreTracking() {
//        return store.allocateStoreTracking();
//    }
//
//    public void setDispatchQueue(DispatchQueue queue) {
//        this.dispatcher = queue;
//    }
//
//
//    /**
//     * @param sqe
//     * @param source
//     * @param delayable
//     */
//    public <T> OperationContext<?> saveQeueuElement(SaveableQueueElement<T> sqe, ISourceController<?> source, boolean delayable, MessageRecordMarshaller<T> marshaller) {
//        return add(new AddElementOperation<T>(sqe, delayable, marshaller), source, !delayable);
//    }
//
//    private class AddElementOperation<T> extends OperationBase<Object> {
//
//        private final SaveableQueueElement<T> op;
//        private MessageRecord record;
//        private boolean delayable;
//        private final MessageRecordMarshaller<T> marshaller;
//
//        public AddElementOperation(SaveableQueueElement<T> op, boolean delayable, MessageRecordMarshaller<T> marshaller) {
//            this.op = op;
//            this.delayable = delayable;
//            if (!delayable) {
//                record = marshaller.marshal(op.getElement());
//                this.marshaller = null;
//            } else {
//                this.marshaller = marshaller;
//            }
//        }
//
//        public boolean isDelayable() {
//            return delayable;
//        }
//
//        @Override
//        public int getLimiterSize() {
//            return op.getLimiterSize() + BASE_MEM_SIZE + 32;
//        }
//
//        @Override
//        protected void doExcecute(Session session) {
//
//            if (record == null) {
//                record = marshaller.marshal(op.getElement());
//            }
//
//            session.messageAdd(record);
//            try {
//                QueueRecord queueRecord = new QueueRecord();
//                queueRecord.setAttachment(null);
//                queueRecord.setMessageKey(record.getId());
//                queueRecord.setSize(record.getSize());
//                queueRecord.setQueueKey(op.getSequenceNumber());
//                session.queueAddMessage(op.getQueueDescriptor(), queueRecord);
//            } catch (KeyNotFoundException e) {
//                e.printStackTrace();
//            }
//        }
//
//        public String toString() {
//            return "AddTxOpOperation " + record.getId() + super.toString();
//        }
//    }
//
//    public long getFlushDelay() {
//        return flushDelay;
//    }
//
//    public void setFlushDelay(long flushDelay) {
//        this.flushDelay = flushDelay;
//    }
//
//    /**
//     * @return true if operations are allowed to bypass the store.
//     */
//    public boolean isStoreBypass() {
//        return storeBypass;
//    }
//
//    /**
//     * Sets if persistent operations should be allowed to bypass the store.
//     * Defaults to true, as this will give you the best performance. In some
//     * cases, you want to disable this as the store being used will double as an
//     * audit log and you do not want any persistent operations to bypass the
//     * store.
//     *
//     * When store bypass is disabled, all {@link Operation#cancel()} requests
//     * will return false.
//     *
//     * @param enable
//     *            if true will enable store bypass
//     */
//    public void setStoreBypass(boolean enable) {
//        this.storeBypass = enable;
//    }
//
//}



/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class UserAlreadyConnectedException extends Exception


/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class BrokerQueueStore { // implements QueueStore<Long, MessageDelivery> {
// TODO:
//    private static final Log LOG = LogFactory.getLog(BrokerQueueStore.class);
//    private static final boolean USE_OLD_QUEUE = false;
//    private static final boolean USE_PRIORITY_QUEUES = true;
//
//    private BrokerDatabase database;
//    private DispatchQueue dispatchQueue;
//
//    private static HashMap<String, ProtocolHandler> protocolHandlers = new HashMap<String, ProtocolHandler>();
//    private static final BrokerDatabase.MessageRecordMarshaller<MessageDelivery> MESSAGE_MARSHALLER = new BrokerDatabase.MessageRecordMarshaller<MessageDelivery>() {
//        /*
//         * (non-Javadoc)
//         *
//         * @see
//         * org.apache.activemq.apollo.broker.BrokerDatabase.MessageRecordMarshaller
//         * #marshal(java.lang.Object)
//         */
//        public MessageRecord marshal(MessageDelivery element) {
//            return element.createMessageRecord();
//        }
//
//        /*
//         * (non-Javadoc)
//         *
//         * @see
//         * org.apache.activemq.apollo.broker.BrokerDatabase.MessageRecordMarshaller
//         * #unMarshall(org.apache.activemq.broker.store.Store.MessageRecord)
//         */
//        public MessageDelivery unMarshall(MessageRecord record, QueueDescriptor queue) {
//            ProtocolHandler handler = protocolHandlers.get(record.getProtocol().toString());
//            if (handler == null) {
//                try {
//                    handler = ProtocolHandlerFactory.createProtocolHandler(record.getProtocol().toString());
//                    protocolHandlers.put(record.getProtocol().toString(), handler);
//                } catch (Throwable thrown) {
//                    throw new RuntimeException("Unknown message format" + record.getProtocol().toString(), thrown);
//                }
//            }
//            try {
//                return handler.createMessageDelivery(record);
//            } catch (IOException ioe) {
//                throw new RuntimeException(ioe);
//            }
//        }
//    };
//
//    final BrokerDatabase.MessageRecordMarshaller<MessageDelivery> getMessageMarshaller() {
//        return MESSAGE_MARSHALLER;
//    }
//
//    private static final Mapper<Long, MessageDelivery> EXPIRATION_MAPPER = new Mapper<Long, MessageDelivery>() {
//        public Long map(MessageDelivery element) {
//            return element.getExpiration();
//        }
//    };
//
//    private static final Mapper<Integer, MessageDelivery> SIZE_MAPPER = new Mapper<Integer, MessageDelivery>() {
//        public Integer map(MessageDelivery element) {
//            return element.getFlowLimiterSize();
//        }
//    };
//
//    public static final Mapper<Integer, MessageDelivery> PRIORITY_MAPPER = new Mapper<Integer, MessageDelivery>() {
//        public Integer map(MessageDelivery element) {
//            return element.getPriority();
//        }
//    };
//
//    static public final Mapper<Long, MessageDelivery> KEY_MAPPER = new Mapper<Long, MessageDelivery>() {
//        public Long map(MessageDelivery element) {
//            return element.getStoreTracking();
//        }
//    };
//
//    static public final Mapper<Integer, MessageDelivery> PARTITION_MAPPER = new Mapper<Integer, MessageDelivery>() {
//        public Integer map(MessageDelivery element) {
//            // we modulo 10 to have at most 10 partitions which the producers
//            // gets split across.
//            return (int) (element.getProducerId().hashCode() % 10);
//        }
//    };
//
//    public static final short SUBPARTITION_TYPE = 0;
//    public static final short SHARED_QUEUE_TYPE = 1;
//    public static final short DURABLE_QUEUE_TYPE = 2;
//    public static short TRANSACTION_QUEUE_TYPE = 3;
//
//    private final HashMap<String, IQueue<Long, MessageDelivery>> sharedQueues = new HashMap<String, IQueue<Long, MessageDelivery>>();
//    private final HashMap<String, IQueue<Long, MessageDelivery>> durableQueues = new HashMap<String, IQueue<Long, MessageDelivery>>();
//
//    private Mapper<Integer, MessageDelivery> partitionMapper;
//
//    private static final int DEFAULT_SHARED_QUEUE_PAGING_THRESHOLD = 1024 * 1024 * 1;
//    private static final int DEFAULT_SHARED_QUEUE_RESUME_THRESHOLD = 1;
//    // Be default we don't page out elements to disk.
//    private static final int DEFAULT_SHARED_QUEUE_SIZE = DEFAULT_SHARED_QUEUE_PAGING_THRESHOLD;
//    //private static final int DEFAULT_SHARED_QUEUE_SIZE = 1024 * 1024 * 10;
//
//    private static long dynamicQueueCounter = 0;
//
//    private static final PersistencePolicy<MessageDelivery> SHARED_QUEUE_PERSISTENCE_POLICY = new PersistencePolicy<MessageDelivery>() {
//
//        private static final boolean PAGING_ENABLED = DEFAULT_SHARED_QUEUE_SIZE > DEFAULT_SHARED_QUEUE_PAGING_THRESHOLD;
//
//        public boolean isPersistent(MessageDelivery elem) {
//            return elem.isPersistent();
//        }
//
//        public boolean isPageOutPlaceHolders() {
//            return true;
//        }
//
//        public boolean isPagingEnabled() {
//            return PAGING_ENABLED;
//        }
//
//        public int getPagingInMemorySize() {
//            return DEFAULT_SHARED_QUEUE_PAGING_THRESHOLD;
//        }
//
//        public boolean isThrottleSourcesToMemoryLimit() {
//            // Keep the queue in memory.
//            return true;
//        }
//
//        public int getDisconnectedThrottleRate() {
//            // By default don't throttle consumers when disconnected.
//            return 0;
//        }
//
//        public int getRecoveryBias() {
//            return 8;
//        }
//    };
//
//    private static final int DEFAULT_DURABLE_QUEUE_PAGING_THRESHOLD = 100 * 1024 * 1;
//    private static final int DEFAULT_DURABLE_QUEUE_RESUME_THRESHOLD = 1;
//    // Be default we don't page out elements to disk.
//    //private static final int DEFAULT_DURABLE_QUEUE_SIZE = DEFAULT_DURABLE_QUEUE_PAGING_THRESHOLD;
//    private static final int DEFAULT_DURABLE_QUEUE_SIZE = 1024 * 1024 * 10;
//
//    private static final PersistencePolicy<MessageDelivery> DURABLE_QUEUE_PERSISTENCE_POLICY = new PersistencePolicy<MessageDelivery>() {
//
//        private static final boolean PAGING_ENABLED = DEFAULT_DURABLE_QUEUE_SIZE > DEFAULT_DURABLE_QUEUE_PAGING_THRESHOLD;
//
//        public boolean isPersistent(MessageDelivery elem) {
//            return elem.isPersistent();
//        }
//
//        public boolean isPageOutPlaceHolders() {
//            return true;
//        }
//
//        public boolean isPagingEnabled() {
//            return PAGING_ENABLED;
//        }
//
//        public int getPagingInMemorySize() {
//            return DEFAULT_DURABLE_QUEUE_PAGING_THRESHOLD;
//        }
//
//        public boolean isThrottleSourcesToMemoryLimit() {
//            // Keep the queue in memory.
//            return true;
//        }
//
//        public int getDisconnectedThrottleRate() {
//            // By default don't throttle consumers when disconnected.
//            return 0;
//        }
//
//        public int getRecoveryBias() {
//            return 8;
//        }
//    };
//
//    public void setDatabase(BrokerDatabase database) {
//        this.database = database;
//    }
//
//    public void setDispatchQueue(DispatchQueue dispatchQueue) {
//        this.dispatchQueue = dispatchQueue;
//    }
//
//    public void loadQueues() throws Exception {
//
//        // Load shared queues
//        Iterator<QueueQueryResult> results = database.listQueues(SHARED_QUEUE_TYPE);
//        while (results.hasNext()) {
//            QueueQueryResult loaded = results.next();
//            IQueue<Long, MessageDelivery> queue = createRestoredQueue(null, loaded);
//            sharedQueues.put(queue.getDescriptor().getQueueName().toString(), queue);
//            LOG.info("Loaded Queue " + queue.getResourceName() + " Messages: " + queue.getEnqueuedCount() + " Size: " + queue.getEnqueuedSize());
//        }
//
//        // Load durable queues
//        results = database.listQueues(DURABLE_QUEUE_TYPE);
//        while (results.hasNext()) {
//            QueueQueryResult loaded = results.next();
//            IQueue<Long, MessageDelivery> queue = createRestoredDurableQueue(loaded);
//            durableQueues.put(queue.getDescriptor().getQueueName().toString(), queue);
//            LOG.info("Loaded Durable " + queue.getResourceName() + " Messages: " + queue.getEnqueuedCount() + " Size: " + queue.getEnqueuedSize());
//
//        }
//    }
//
//    private IQueue<Long, MessageDelivery> createRestoredQueue(IPartitionedQueue<Long, MessageDelivery> parent, QueueQueryResult loaded) throws IOException {
//
//        IQueue<Long, MessageDelivery> queue;
//        if (parent != null) {
//            queue = parent.createPartition(loaded.getDescriptor().getPartitionKey());
//        } else {
//            queue = createSharedQueueInternal(loaded.getDescriptor().getQueueName().toString(), loaded.getDescriptor().getQueueType());
//        }
//
//        queue.initialize(loaded.getFirstSequence(), loaded.getLastSequence(), loaded.getCount(), loaded.getSize());
//
//        // Creat the child queues
//        Collection<QueueQueryResult> children = loaded.getPartitions();
//        if (children != null) {
//            try {
//                IPartitionedQueue<Long, MessageDelivery> pQueue = (IPartitionedQueue<Long, MessageDelivery>) queue;
//                for (QueueQueryResult child : children) {
//                    createRestoredQueue(pQueue, child);
//                }
//            } catch (ClassCastException cce) {
//                LOG.error("Loaded partition for unpartitionable queue: " + queue.getResourceName());
//                throw cce;
//            }
//        }
//
//        return queue;
//
//    }
//
//    private IQueue<Long, MessageDelivery> createRestoredDurableQueue(QueueQueryResult loaded) throws IOException {
//
//        ExclusivePersistentQueue<Long, MessageDelivery> queue = createDurableQueueInternal(loaded.getDescriptor().getQueueName().toString(), loaded.getDescriptor().getQueueType());
//        queue.initialize(loaded.getFirstSequence(), loaded.getLastSequence(), loaded.getCount(), loaded.getSize());
//
//        //TODO implement this for priority queue:
//        // Create the child queues
//        /*
//         * Collection<QueueQueryResult> children = loaded.getPartitions(); if
//         * (children != null) { try { IPartitionedQueue<Long, MessageDelivery>
//         * pQueue = (IPartitionedQueue<Long, MessageDelivery>) queue; for
//         * (QueueQueryResult child : children) { createRestoredQueue(pQueue,
//         * child); } } catch (ClassCastException cce) {
//         * LOG.error("Loaded partition for unpartitionable queue: " +
//         * queue.getResourceName()); throw cce; } }
//         */
//
//        return queue;
//
//    }
//
//    public IQueue<Long, MessageDelivery> getQueue(AsciiBuffer queueName) {
//        //TODO
//        return null;
//    }
//
//    public Collection<IQueue<Long, MessageDelivery>> getSharedQueues() {
//        synchronized (this) {
//            Collection<IQueue<Long, MessageDelivery>> c = sharedQueues.values();
//            ArrayList<IQueue<Long, MessageDelivery>> ret = new ArrayList<IQueue<Long, MessageDelivery>>(c.size());
//            ret.addAll(c);
//            return ret;
//        }
//    }
//
//    public IQueue<Long, MessageDelivery> createDurableQueue(String name) {
//        IQueue<Long, MessageDelivery> queue = null;
//        synchronized (this) {
//            queue = durableQueues.get(name);
//            if (queue == null) {
//                queue = createDurableQueueInternal(name, USE_PRIORITY_QUEUES ? QueueDescriptor.SHARED_PRIORITY : QueueDescriptor.SHARED);
//                queue.getDescriptor().setApplicationType(DURABLE_QUEUE_TYPE);
//                queue.initialize(0, 0, 0, 0);
//                durableQueues.put(name, queue);
//                addQueue(queue.getDescriptor());
//            }
//        }
//
//        return queue;
//    }
//
//    public ExclusivePersistentQueue<Long, MessageDelivery> createExclusivePersistentQueue() {
//        ExclusivePersistentQueue<Long, MessageDelivery> queue = null;
//        synchronized (this) {
//            String name = "temp:" + (dynamicQueueCounter++);
//            queue = createDurableQueueInternal(name, USE_PRIORITY_QUEUES ? QueueDescriptor.SHARED_PRIORITY : QueueDescriptor.SHARED);
//            queue.getDescriptor().setApplicationType(DURABLE_QUEUE_TYPE);
//            queue.initialize(0, 0, 0, 0);
//            addQueue(queue.getDescriptor());
//        }
//        return queue;
//    }
//
//    public Collection<IQueue<Long, MessageDelivery>> getDurableQueues() {
//        synchronized (this) {
//            Collection<IQueue<Long, MessageDelivery>> c = durableQueues.values();
//            ArrayList<IQueue<Long, MessageDelivery>> ret = new ArrayList<IQueue<Long, MessageDelivery>>(c.size());
//            ret.addAll(c);
//            return ret;
//        }
//    }
//
//    public IQueue<Long, MessageDelivery> createSharedQueue(String name) {
//
//        IQueue<Long, MessageDelivery> queue = null;
//        synchronized (this) {
//            queue = sharedQueues.get(name);
//            if (queue == null) {
//                queue = createSharedQueueInternal(name, USE_PRIORITY_QUEUES ? QueueDescriptor.SHARED_PRIORITY : QueueDescriptor.SHARED);
//                queue.getDescriptor().setApplicationType(SHARED_QUEUE_TYPE);
//                queue.initialize(0, 0, 0, 0);
//                sharedQueues.put(name, queue);
//                addQueue(queue.getDescriptor());
//            }
//        }
//
//        return queue;
//    }
//
//    private ExclusivePersistentQueue<Long, MessageDelivery> createDurableQueueInternal(final String name, short type) {
//        ExclusivePersistentQueue<Long, MessageDelivery> queue;
//
//        SizeLimiter<MessageDelivery> limiter = new SizeLimiter<MessageDelivery>(DEFAULT_DURABLE_QUEUE_SIZE, DEFAULT_DURABLE_QUEUE_RESUME_THRESHOLD) {
//            @Override
//            public int getElementSize(MessageDelivery elem) {
//                return elem.getFlowLimiterSize();
//            }
//        };
//        queue = new ExclusivePersistentQueue<Long, MessageDelivery>(name, limiter);
//        queue.setStore(this);
//        queue.setPersistencePolicy(DURABLE_QUEUE_PERSISTENCE_POLICY);
//        queue.setExpirationMapper(EXPIRATION_MAPPER);
//        return queue;
//    }
//
//    private IQueue<Long, MessageDelivery> createSharedQueueInternal(final String name, short type) {
//
//        IQueue<Long, MessageDelivery> ret;
//
//        switch (type) {
//        case QueueDescriptor.PARTITIONED: {
//            PartitionedQueue<Long, MessageDelivery> queue = new PartitionedQueue<Long, MessageDelivery>(name) {
//                @Override
//                public IQueue<Long, MessageDelivery> createPartition(int partitionKey) {
//                    IQueue<Long, MessageDelivery> queue = createSharedQueueInternal(name + "$" + partitionKey, USE_PRIORITY_QUEUES ? QueueDescriptor.SHARED_PRIORITY : QueueDescriptor.SHARED);
//                    queue.getDescriptor().setPartitionId(partitionKey);
//                    queue.getDescriptor().setParent(this.getDescriptor().getQueueName());
//                    return queue;
//                }
//
//            };
//            queue.setPartitionMapper(partitionMapper);
//
//            ret = queue;
//            break;
//        }
//        case QueueDescriptor.SHARED_PRIORITY: {
//            PrioritySizeLimiter<MessageDelivery> limiter = new PrioritySizeLimiter<MessageDelivery>(DEFAULT_SHARED_QUEUE_SIZE, DEFAULT_SHARED_QUEUE_RESUME_THRESHOLD, 10);
//            limiter.setPriorityMapper(PRIORITY_MAPPER);
//            limiter.setSizeMapper(SIZE_MAPPER);
//            SharedPriorityQueue<Long, MessageDelivery> queue = new SharedPriorityQueue<Long, MessageDelivery>(name, limiter);
//            ret = queue;
//            queue.setKeyMapper(KEY_MAPPER);
//            queue.setAutoRelease(true);
//            break;
//        }
//        case QueueDescriptor.SHARED: {
//            SizeLimiter<MessageDelivery> limiter = new SizeLimiter<MessageDelivery>(DEFAULT_SHARED_QUEUE_SIZE, DEFAULT_SHARED_QUEUE_RESUME_THRESHOLD) {
//                @Override
//                public int getElementSize(MessageDelivery elem) {
//                    return elem.getFlowLimiterSize();
//                }
//            };
//
//            if (!USE_OLD_QUEUE) {
//                SharedQueue<Long, MessageDelivery> sQueue = new SharedQueue<Long, MessageDelivery>(name, limiter);
//                sQueue.setKeyMapper(KEY_MAPPER);
//                sQueue.setAutoRelease(true);
//                ret = sQueue;
//            } else {
//                SharedQueueOld<Long, MessageDelivery> sQueue = new SharedQueueOld<Long, MessageDelivery>(name, limiter);
//                sQueue.setKeyMapper(KEY_MAPPER);
//                sQueue.setAutoRelease(true);
//                ret = sQueue;
//            }
//            break;
//        }
//        default: {
//            throw new IllegalArgumentException("Unknown queue type" + type);
//        }
//        }
//        ret.getDescriptor().setApplicationType(SUBPARTITION_TYPE);
//        ret.setStore(this);
//        ret.setPersistencePolicy(SHARED_QUEUE_PERSISTENCE_POLICY);
//        ret.setExpirationMapper(EXPIRATION_MAPPER);
//
//        return ret;
//    }
//
//    public final void deleteQueueElement(SaveableQueueElement<MessageDelivery> sqe) {
//        MessageDelivery md = sqe.getElement();
//        //If the message delivery isn't null, funnel through it
//        //since the message may not yet be in the store:
//        if (md != null) {
//            md.acknowledge(sqe);
//        } else {
//            database.deleteQueueElement(sqe);
//        }
//
//    }
//
//    public final boolean isFromStore(MessageDelivery elem) {
//        return elem.isFromStore();
//    }
//
//    public final void persistQueueElement(SaveableQueueElement<MessageDelivery> elem, ISourceController<?> controller, boolean delayable) {
//        elem.getElement().persist(elem, controller, delayable);
//    }
//
//    public final void restoreQueueElements(QueueDescriptor queue, boolean recordsOnly, long firstSequence, long maxSequence, int maxCount, RestoreListener<MessageDelivery> listener) {
//        database.restoreQueueElements(queue, recordsOnly, firstSequence, maxSequence, maxCount, listener, MESSAGE_MARSHALLER);
//    }
//
//    public final void addQueue(QueueDescriptor queue) {
//        database.addQueue(queue);
//    }
//
//    public final void deleteQueue(QueueDescriptor queue) {
//        database.deleteQueue(queue);
//    }

    def setDatabase(database:Store ) = {
    }

    def setDispatchQueue(dispatchQueue:DispatchQueue )= {
    }

    def loadQueues() ={
    }
}