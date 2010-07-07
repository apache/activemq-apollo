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
import org.scalatest.Informer
import org.fusesource.hawtdispatch.BaseRetained
import java.util.concurrent.{CountDownLatch, TimeUnit}
import org.apache.activemq.apollo.broker._


object BaseBrokerPerfSupport {
  var PERFORMANCE_SAMPLES = Integer.parseInt(System.getProperty("PERFORMANCE_SAMPLES", "3"))
  var IO_WORK_AMOUNT = 0
  var FANIN_COUNT = 10
  var FANOUT_COUNT = 10
  var PRIORITY_LEVELS = 10
  var USE_INPUT_QUEUES = true

  var USE_KAHA_DB = true;
  var PURGE_STORE = true;
  var PERSISTENT = false;
  var DURABLE = false;

}

abstract class BaseBrokerPerfSupport extends FunSuiteSupport {
  import BaseBrokerPerfSupport._

  // Set to put senders and consumers on separate brokers.
  protected var multibroker = false;

  // Set to mockup up ptp:
  protected var ptp = false;

  // Set to use tcp IO
  protected var tcp = true;
  // set to force marshalling even in the NON tcp case.
  protected var forceMarshalling = true;

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


  override protected def beforeAll(configMap: Map[String, Any]) = {
    super.beforeAll(configMap)
    if (tcp) {
      sendBrokerBindURI = "tcp://localhost:10000?wireFormat=" + getBrokerWireFormat();
      receiveBrokerBindURI = "tcp://localhost:20000?wireFormat=" + getBrokerWireFormat();

      sendBrokerConnectURI = "tcp://localhost:10000?wireFormat=" + getRemoteWireFormat();
      receiveBrokerConnectURI = "tcp://localhost:20000?wireFormat=" + getRemoteWireFormat();
    } else {
      sendBrokerConnectURI = "pipe://SendBroker";
      receiveBrokerConnectURI = "pipe://ReceiveBroker";
      if (forceMarshalling) {
        sendBrokerBindURI = sendBrokerConnectURI + "?wireFormat=" + getBrokerWireFormat();
        receiveBrokerBindURI = receiveBrokerConnectURI + "?wireFormat=" + getBrokerWireFormat();
      } else {
        sendBrokerBindURI = sendBrokerConnectURI;
        receiveBrokerBindURI = receiveBrokerConnectURI;
      }
    }
  }

  def getBrokerWireFormat() = "multi"

  def getRemoteWireFormat(): String

  if (!ptp) {
    test("1 producer -> 1 destination -> 0 consumers") {
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

  test("1 producer -> 1 destination -> 1 consumers") {
    producerCount = 1;
    destCount = 1;
    consumerCount = 1;

    createConnections();
//    producers.get(0).thinkTime = 500000*1000;

    // Start 'em up.
    startClients();
    try {
      reportRates();
    } finally {
      stopServices();
    }
  }

  test(format("%d producers -> 1 destination -> %d consumers", FANIN_COUNT, FANOUT_COUNT)) {
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

  test(format("%d producers -> 1 destination -> 1 consumer", FANIN_COUNT)) {
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

  test(format("1 producer -> 1 destination -> %d consumers", FANOUT_COUNT)) {
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

  test("2 producer -> 2 destination -> 2 consumers") {
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

  test("10 producers -> 10 destinations -> 10 consumers") {
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
   * Tests 2 producers sending to 1 destination with 2 consumres, but with
   * consumers set to select only messages from each producer. 1 consumers is
   * set to slow, the other producer should be able to send quickly.
   *
   * @throws Exception
   */
  test("2 producer -> 2 destination -> 2 slow consumers") {
    producerCount = 2;
    destCount = 2;
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

  test("2 producer -> 2 destination -> 2 selector consumers") {
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
  test("1 high and 1 normal priority producer -> 1 destination -> 1 consumer") {
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
        Thread.sleep(1000 * 5);
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
  test("1 high/mixed and 1 normal priority producer -> 1 destination -> 1 consumer") {
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
        Thread.sleep(1000 * 5);
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
    println("Checking "+(if (ptp) "ptp" else "topic")+" rates...");
    for (i <- 0 until PERFORMANCE_SAMPLES) {
      var p = new Period();
      Thread.sleep(1000 * 5);
      println(totalProducerRate.getRateSummary(p));
      println(totalConsumerRate.getRateSummary(p));
      totalProducerRate.reset();
      totalConsumerRate.reset();
    }
  }

  def createConnections() = {

    if (multibroker) {
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
      val domain = if (ptp) {Domain.QUEUE_DOMAIN} else {Domain.TOPIC_DOMAIN}
      val name = new AsciiBuffer("dest" + (i + 1))
      var bean = new SingleDestination(domain, name)
      dests(i) = bean;
      if (ptp) {
        sendBroker.defaultVirtualHost.createQueue(dests(i));
        if (multibroker) {
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
    val tracker = new CompletionTracker
    for (broker <- brokers) {
      broker.stop(tracker.task());
    }
    brokers.clear
    for (connection <- producers) {
      connection.stop(tracker.task());
    }
    producers.clear
    for (connection <- consumers) {
      connection.stop(tracker.task());
    }
    consumers.clear
    println("waiting for services to stop");
    tracker.await
    stopping.set(false)
  }

  private def startBrokers() = {
    val tracker = new CompletionTracker
    for (broker <- brokers) {
      broker.start(tracker.task());
    }
    tracker.await
  }


  private def startClients() = {
    var tracker = new CompletionTracker
    for (connection <- consumers) {
      connection.start(tracker.task());
    }
    tracker.await
    tracker = new CompletionTracker
    for (connection <- producers) {
      connection.start(tracker.task());
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
  var brokerPerfTest:BaseBrokerPerfSupport = null

  override def start(onComplete:Runnable) = {
    consumerRate.name("Consumer " + name + " Rate");
    totalConsumerRate.add(consumerRate);
    transport = TransportFactory.connect(uri);
    super.start(onComplete);
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

  override def start(onComplete:Runnable) = {

    if (payloadSize > 0) {
      var sb = new StringBuilder(payloadSize);
      for (i <- 0 until payloadSize) {
        sb.append(('a' + (i % 26)).toChar);
      }
      filler = sb.toString();
    }

    rate.name("Producer " + name + " Rate");
    totalProducerRate.add(rate);

    transport = TransportFactory.connect(uri);
    super.start(onComplete);

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