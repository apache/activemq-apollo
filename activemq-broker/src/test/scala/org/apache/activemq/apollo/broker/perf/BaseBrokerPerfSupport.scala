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
package org.apache.activemq.apollo.broker.perf

import _root_.java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import _root_.org.apache.activemq.metric.{Period, MetricAggregator, MetricCounter}
import _root_.java.lang.{String}
import _root_.org.junit.{Test, Before}

import org.apache.activemq.transport.TransportFactory

import _root_.scala.collection.JavaConversions._
import _root_.org.fusesource.hawtdispatch.ScalaDispatch._
import org.apache.activemq.util.buffer.AsciiBuffer
import org.apache.activemq.broker.store.{Store, StoreFactory}
import java.io.{File, IOException}
import java.util.ArrayList
import org.fusesource.hawtdispatch.BaseRetained
import java.util.concurrent.{CountDownLatch, TimeUnit}
import org.apache.activemq.apollo.broker._
import org.scalatest._

object BaseBrokerPerfSupport {
  var PERFORMANCE_SAMPLES = Integer.parseInt(System.getProperty("PERFORMANCE_SAMPLES", "1"))
  var SAMPLE_PERIOD = java.lang.Long.parseLong(System.getProperty("SAMPLE_PERIOD", "1000"))
  var IO_WORK_AMOUNT = 0
  var FANIN_COUNT = 10
  var FANOUT_COUNT = 10
  var PRIORITY_LEVELS = 10
  var USE_INPUT_QUEUES = true

  var USE_KAHA_DB = true;
  var PURGE_STORE = true;
  var PERSISTENT = false;
  var DURABLE = false;

  // Set to test against ptp queues instead of topics:
  var PTP = false;

  // Set to put senders and consumers on separate brokers.
  var MULTI_BROKER = false;

  // Set to use tcp IO
  protected var TCP = true;

  // set to force marshalling even in the NON tcp case.
  protected var FORCE_MARSHALLING = true;
}

abstract class BaseBrokerPerfSupport extends FunSuiteSupport with BeforeAndAfterEach {
  import BaseBrokerPerfSupport._


  protected var sendBrokerBindURI: String = null
  protected var receiveBrokerBindURI: String = null
  protected var sendBrokerConnectURI: String = null
  protected var receiveBrokerConnectURI: String = null

  protected var producerCount = 0
  protected var consumerCount = 0
  protected var destCount = 0

  protected val totalProducerRate = new MetricAggregator().name("Aggregate Producer Rate").unit("items")
  protected val totalConsumerRate = new MetricAggregator().name("Aggregate Consumer Rate").unit("items")

  protected var sendBroker: Broker = null
  protected var rcvBroker: Broker = null
  protected val brokers = new ArrayList[Broker]()
  protected val msgIdGenerator = new AtomicLong()
  val stopping = new AtomicBoolean()

  val producers = new ArrayList[RemoteProducer]()
  val consumers = new ArrayList[RemoteConsumer]()

  var spread_sheet_stats:List[(String, Any)] = Nil


  override protected def beforeEach() = {
    totalProducerRate.removeAllMetrics
    totalConsumerRate.removeAllMetrics
    brokers.clear
    producers.clear
    consumers.clear
    stopping.set(false)
    rcvBroker=null
    sendBroker=null
    producerCount = 0
    consumerCount = 0
    destCount =0
  }

  override protected def beforeAll(configMap: Map[String, Any]) = {
    super.beforeAll(configMap)
    if (TCP) {
      sendBrokerBindURI = "tcp://localhost:10000?wireFormat=" + getBrokerWireFormat();
      receiveBrokerBindURI = "tcp://localhost:20000?wireFormat=" + getBrokerWireFormat();

      sendBrokerConnectURI = "tcp://localhost:10000?wireFormat=" + getRemoteWireFormat();
      receiveBrokerConnectURI = "tcp://localhost:20000?wireFormat=" + getRemoteWireFormat();
    } else {
      sendBrokerConnectURI = "pipe://SendBroker";
      receiveBrokerConnectURI = "pipe://ReceiveBroker";
      if (FORCE_MARSHALLING) {
        sendBrokerBindURI = sendBrokerConnectURI + "?wireFormat=" + getBrokerWireFormat();
        receiveBrokerBindURI = receiveBrokerConnectURI + "?wireFormat=" + getBrokerWireFormat();
      } else {
        sendBrokerBindURI = sendBrokerConnectURI;
        receiveBrokerBindURI = receiveBrokerConnectURI;
      }
    }
  }


  override protected def afterEach() = {
    println("Spread sheet stats:")
    println(spread_sheet_stats.map(_._1).mkString(","))
    println(spread_sheet_stats.map(_._2).mkString(","))
  }

  override protected def afterAll() = {
    println("Spread sheet stats:")
    println(spread_sheet_stats.map(_._1).mkString(","))
    println(spread_sheet_stats.map(_._2).mkString(","))
  }

  def getBrokerWireFormat() = "multi"

  def getRemoteWireFormat(): String

  /**
   * Used to benchmark what is the raw speed of sending messages one way.
   * Divide by 2 and compare against 1-1-1 to figure out what the broker dispatching
   * overhead is.
   */
  if (!PTP) {
    test("1->1->0") {
      producerCount = 1;
      destCount = 1;

      createConnections();

      // Start 'em up.
      startClients();
      try {
        reportRates();
      } finally {
        stopServices();
      }
    }
  }

  /**
   * The baseline of the performance of going from 1 producer to 1 consumer.
   */
  test("1->1->1") {
    println(testName)
    producerCount = 1;
    destCount = 1;
    consumerCount = 1;

    createConnections();
    // Start 'em up.
    startClients();
    try {
      reportRates();
    } finally {
      stopServices();
    }
  }

  /**
   * To compare against the performance of the 1-1-1 case... If you have
   * linear scalability then, this should be twice as fast.
   */
  test("2->2->2") {
    producerCount = 2;
    destCount = 2;
    consumerCount = 2;

    createConnections();

    // Start 'em up.
    startClients();
    try {
      reportRates();
    } finally {
      stopServices();
    }
  }

  /**
   * To see how high producer and consumer contention on a destination performs.
   */
  test(format("%d->1->%d", FANIN_COUNT, FANOUT_COUNT)) {
    producerCount = FANIN_COUNT;
    consumerCount = FANOUT_COUNT;
    destCount = 1;

    createConnections();

    // Start 'em up.
    startClients();
    try {
      reportRates();
    } finally {
      stopServices();
    }
  }

  /**
   * To see how high producer contention on a destination performs.
   */
  test(format("%d->1->1", FANIN_COUNT)) {
    producerCount = FANIN_COUNT;
    destCount = 1;
    consumerCount = 1;

    createConnections();

    // Start 'em up.
    startClients();
    try {
      reportRates();
    } finally {
      stopServices();
    }
  }

  /**
   * To see how high consumer contention on a destination performs.
   */
  test(format("1->1->%d", FANOUT_COUNT)) {
    producerCount = 1;
    destCount = 1;
    consumerCount = FANOUT_COUNT;

    createConnections();

    // Start 'em up.
    startClients();
    try {
      reportRates();
    } finally {
      stopServices();
    }
  }

  /**
   * To test how an overload situation affects scalability.  Compare to the
   * scalability trend of 1-1-1 to 2-2-2
   */
  test("10->10->10") {
    producerCount = 10;
    destCount = 10;
    consumerCount = 10;

    createConnections();

    // Start 'em up.
    startClients();
    try {
      reportRates();
    } finally {
      stopServices();
    }
  }

  /**
   *  Tests 1 producers sending to 1 destination with 1 slow and 1 fast consumer.
   *
   * queue case: the producer should not slow down since it can dispatch to the
   *             fast consumer
   *
   * topic case: the producer should slow down since it HAS to dispatch to the
   *             slow consumer.
   *
   */
  test("1->1->[1 slow,1 fast]") {
    producerCount = 2;
    destCount = 1;
    consumerCount = 2;

    createConnections();
    consumers.get(0).thinkTime = 50;

    // Start 'em up.
    startClients();
    try {
      reportRates();
    } finally {
      stopServices();
    }
  }

  test("2->2->[1,1 selecting]") {
    producerCount = 2;
    destCount = 2;
    consumerCount = 2;

    createConnections();

    // Add properties to match producers to their consumers
    for (i <- 0 until consumerCount) {
      var property = "match" + i;
      consumers.get(i).selector = property;
      producers.get(i).property = property;
    }

    // Start 'em up.
    startClients();
    try {
      reportRates();
    } finally {
      stopServices();
    }
  }

  /**
   * Test sending with 1 high priority sender. The high priority sender should
   * have higher throughput than the other low priority senders.
   *
   * @throws Exception
   */
  test("[1 high, 1 normal]->1->1") {
    producerCount = 2;
    destCount = 1;
    consumerCount = 1;

    createConnections();
    var producer = producers.get(0);
    producer.priority = 1
    producer.rate.setName("High Priority Producer Rate");

    consumers.get(0).thinkTime = 1;

    // Start 'em up.
    startClients();
    try {
      println("Checking rates...");
      for (i <- 0 until PERFORMANCE_SAMPLES) {
        var p = new Period();
        Thread.sleep(SAMPLE_PERIOD);
        println(producer.rate.getRateSummary(p));
        println(totalProducerRate.getRateSummary(p));
        println(totalConsumerRate.getRateSummary(p));
        totalProducerRate.reset();
        totalConsumerRate.reset();
      }

    } finally {
      stopServices();
    }
  }

  /**
   * Test sending with 1 high priority sender. The high priority sender should
   * have higher throughput than the other low priority senders.
   *
   * @throws Exception
   */
  test("[1 high, 1 mixed, 1 normal]->1->1") {
    producerCount = 2;
    destCount = 1;
    consumerCount = 1;

    createConnections();
    var producer = producers.get(0);
    producer.priority = 1;
    producer.priorityMod = 3;
    producer.rate.setName("High Priority Producer Rate");

    consumers.get(0).thinkTime = 1

    // Start 'em up.
    startClients();
    try {

      println("Checking rates...");
      for (i <- 0 until PERFORMANCE_SAMPLES) {
        var p = new Period();
        Thread.sleep(SAMPLE_PERIOD);
        println(producer.rate.getRateSummary(p));
        println(totalProducerRate.getRateSummary(p));
        println(totalConsumerRate.getRateSummary(p));
        totalProducerRate.reset();
        totalConsumerRate.reset();
      }

    } finally {
      stopServices();
    }
  }

  def reportRates() = {
    val best_sample = PERFORMANCE_SAMPLES/2

    println("Checking "+(if (PTP) "ptp" else "topic")+" rates...");
    for (i <- 0 until PERFORMANCE_SAMPLES) {
      var p = new Period();
      Thread.sleep(SAMPLE_PERIOD);
      if( producerCount > 0 ) {
        println(totalProducerRate.getRateSummary(p));
      }
      if( consumerCount > 0 ) {
        println(totalConsumerRate.getRateSummary(p));
      }

      if( i == best_sample ) {
        if( producerCount > 0 ) {
          spread_sheet_stats = spread_sheet_stats ::: ( testName+" :: producer rate", totalProducerRate.total(p) ) :: Nil
          if( producerCount > 1 ) {
            spread_sheet_stats = spread_sheet_stats ::: ( testName+" :: producer deviation", totalProducerRate.deviation ) :: Nil
          }
        }
        if( consumerCount > 0 ) {
          spread_sheet_stats = spread_sheet_stats ::: ( testName+" :: consumer rate", totalConsumerRate.total(p) ) :: Nil
          if( consumerCount > 1 ) {
            spread_sheet_stats = spread_sheet_stats ::: ( testName+" :: consumer deviation", totalConsumerRate.deviation ) :: Nil
          }
        }
      }
      
      totalProducerRate.reset();
      totalConsumerRate.reset();
    }


  }

  def createConnections() = {

    if (MULTI_BROKER) {
      sendBroker = createBroker("SendBroker", sendBrokerBindURI, sendBrokerConnectURI);
      rcvBroker = createBroker("RcvBroker", receiveBrokerBindURI, receiveBrokerConnectURI);
      brokers.add(sendBroker);
      brokers.add(rcvBroker);
    } else {
      sendBroker = createBroker("Broker", sendBrokerBindURI, sendBrokerConnectURI);
      rcvBroker = sendBroker
      brokers.add(sendBroker);
    }

    startBrokers();

    var dests = new Array[Destination](destCount);

    for (i <- 0 until destCount) {
      val domain = if (PTP) {Domain.QUEUE_DOMAIN} else {Domain.TOPIC_DOMAIN}
      val name = new AsciiBuffer("dest" + (i + 1))
      var bean = new SingleDestination(domain, name)
      dests(i) = bean;
      if (PTP) {
        sendBroker.defaultVirtualHost.createQueue(dests(i));
        if (MULTI_BROKER) {
          rcvBroker.defaultVirtualHost.createQueue(dests(i));
        }
      }
    }

    for (i <- 0 until producerCount) {
      var destination = dests(i % destCount);
      var producer = createProducer(i, destination);
      producer.persistentDelivery = PERSISTENT;
      producers.add(producer);
    }

    for (i <- 0 until consumerCount) {
      var destination = dests(i % destCount);
      var consumer = createConsumer(i, destination);
      consumer.durable = DURABLE;
      consumers.add(consumer);
    }

    // Create MultiBroker connections:
    // if (multibroker) {
    // Pipe<Message> pipe = new Pipe<Message>();
    // sendBroker.createBrokerConnection(rcvBroker, pipe);
    // rcvBroker.createBrokerConnection(sendBroker, pipe.connect());
    // }
  }

  def createConsumer(i: Int, destination: Destination): RemoteConsumer = {

    var consumer = createConsumer();
    consumer.brokerPerfTest = this

    consumer.uri = rcvBroker.connectUris.head
    consumer.destination = destination
    consumer.name = "consumer" + (i + 1)
    consumer.totalConsumerRate = totalConsumerRate
    return consumer;
  }

  protected def createConsumer(): RemoteConsumer

  private def createProducer(id: Int, destination: Destination): RemoteProducer = {
    var producer = createProducer();
    producer.brokerPerfTest = this
    producer.uri = sendBroker.connectUris.head
    producer.producerId = id + 1
    producer.name = "producer" + (id + 1)
    producer.destination = destination
    producer.messageIdGenerator = msgIdGenerator
    producer.totalProducerRate = totalProducerRate
    producer
  }

  protected def createProducer(): RemoteProducer

  private def createBroker(name: String, bindURI: String, connectUri: String): Broker = {
    val broker = new Broker()
    broker.transportServers.add(TransportFactory.bind(bindURI))
    broker.connectUris.add(connectUri)
    broker
  }

  protected def createStore(broker: Broker): Store = {
    val store = if (USE_KAHA_DB) {
      StoreFactory.createStore("kaha-db");
    } else {
      StoreFactory.createStore("memory");
    }
    store.setStoreDirectory(new File("target/test-data/broker-test/" + broker.name));
    store.setDeleteAllMessages(PURGE_STORE);
    store
  }

  private def stopServices() = {
    stopping.set(true);
    val tracker = new CompletionTracker("test shutdown")
    for (broker <- brokers) {
      broker.stop(tracker.task("broker"));
    }
    for (connection <- producers) {
      connection.stop(tracker.task(connection.toString));
    }
    for (connection <- consumers) {
      connection.stop(tracker.task(connection.toString));
    }
    println("waiting for services to stop");
    tracker.await
  }

  private def startBrokers() = {
    val tracker = new CompletionTracker("test broker startup")
    for (broker <- brokers) {
      broker.start(tracker.task("broker"));
    }
    tracker.await
  }


  private def startClients() = {
    var tracker = new CompletionTracker("test consumer startup")
    for (connection <- consumers) {
      connection.start(tracker.task(connection.toString));
    }
    tracker.await
    tracker = new CompletionTracker("test producer startup")
    for (connection <- producers) {
      connection.start(tracker.task(connection.toString));
    }
    tracker.await
  }

}

abstract class RemoteConsumer extends Connection {
  val consumerRate = new MetricCounter();
  var totalConsumerRate: MetricAggregator = null
  var thinkTime: Long = 0
  var destination: Destination = null
  var selector: String = null;
  var durable = false;
  var uri: String = null
  var name:String = null
  var brokerPerfTest:BaseBrokerPerfSupport = null

  override protected def _start(onComplete:Runnable) = {
    if( consumerRate.getName == null ) {
      consumerRate.name("Consumer " + name + " Rate");
    }
    totalConsumerRate.add(consumerRate);
    transport = TransportFactory.connect(uri);
    super._start(onComplete);
  }


  override def onTransportConnected() = {
    setupSubscription();
    transport.resumeRead
  }

  override def onTransportFailure(error: IOException) = {
    if (!brokerPerfTest.stopping.get()) {
      System.err.println("Client Async Error:");
      error.printStackTrace();
    }
  }

  protected def setupSubscription()

}


abstract class RemoteProducer extends Connection {
  val rate = new MetricCounter();

  var name:String = null
  var messageIdGenerator: AtomicLong = null
  var priority = 0
  var persistentDelivery = false
  var priorityMod = 0
  var counter = 0
  var producerId = 0
  var destination: Destination = null
  var property: String = null
  var totalProducerRate: MetricAggregator = null
  var next: Delivery = null
  var thinkTime: Long = 0

  var filler: String = null
  var payloadSize = 20
  var uri: String = null
  var brokerPerfTest:BaseBrokerPerfSupport = null

  override def onTransportFailure(error: IOException) = {
    if (!brokerPerfTest.stopping.get()) {
      System.err.println("Client Async Error:");
      error.printStackTrace();
    }
  }

  override protected def _start(onComplete:Runnable) = {

    if (payloadSize > 0) {
      var sb = new StringBuilder(payloadSize);
      for (i <- 0 until payloadSize) {
        sb.append(('a' + (i % 26)).toChar);
      }
      filler = sb.toString();
    }

    if( rate.getName == null ) {
      rate.name("Producer " + name + " Rate");
    }
    totalProducerRate.add(rate);

    transport = TransportFactory.connect(uri);
    super._start(onComplete);

  }

  override def onTransportConnected() = {
    setupProducer();
    transport.resumeRead
  }

  def setupProducer()

def createPayload(): String = {
    if (payloadSize >= 0) {
      var sb = new StringBuilder(payloadSize);
      sb.append(name);
      sb.append(':');
      counter += 1
      sb.append(counter);
      sb.append(':');
      var length = sb.length;
      if (length <= payloadSize) {
        sb.append(filler.subSequence(0, payloadSize - length));
        return sb.toString();
      } else {
        return sb.substring(0, payloadSize);
      }
    } else {
      counter += 1
      return name + ":" + (counter);
    }
  }

}