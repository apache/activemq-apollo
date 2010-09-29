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
import _root_.java.lang.{String}

import org.apache.activemq.apollo.broker._
import org.scalatest._
import java.io.{File, IOException}
import org.apache.activemq.apollo.util.metric.{Period, MetricAggregator, MetricCounter}
import org.fusesource.hawtbuf.AsciiBuffer
import collection.mutable.ListBuffer
import java.net.URL
import java.util.concurrent.TimeUnit
import org.fusesource.hawtdispatch.ScalaDispatch._
import org.apache.activemq.apollo.dto.BrokerDTO
import org.apache.activemq.apollo.transport.TransportFactory
import org.apache.activemq.apollo.util._

/**
 * 
 */
abstract class BrokerPerfSupport extends FunSuiteSupport with BeforeAndAfterEach {

  var PERFORMANCE_SAMPLES = Integer.parseInt(System.getProperty("PERFORMANCE_SAMPLES", "6"))
  var SAMPLE_PERIOD = java.lang.Long.parseLong(System.getProperty("SAMPLE_PERIOD", "1000"))

  protected var TCP = true // Set to use tcp IO

  var USE_KAHA_DB = true
  var PURGE_STORE = true

  // Set to put senders and consumers on separate brokers.
  var MULTI_BROKER = false

  var DUMP_REPORT_COLS = true


  var PTP = false
  var PERSISTENT = false
  var DURABLE = false
  var MESSAGE_SIZE = 20


  protected var sendBrokerBindURI: String = null
  protected var receiveBrokerBindURI: String = null
  protected var sendBrokerConnectURI: String = null
  protected var receiveBrokerConnectURI: String = null

  protected var producerCount = 0
  protected var consumerCount = 0
  protected var destCount = 0

  protected var totalProducerRate:MetricAggregator = null
  protected var totalConsumerRate:MetricAggregator = null
  var totalMessageSent = 0L
  var totalMessageReceived = 0L

  protected var sendBroker: Broker = null
  protected var rcvBroker: Broker = null
  protected val brokers = ListBuffer[Broker]()
  protected val msgIdGenerator = new AtomicLong()
  val stopping = new AtomicBoolean()

  val producers = ListBuffer[RemoteProducer]()
  val consumers = ListBuffer[RemoteConsumer]()

  var samples:List[(String, AnyRef)] = Nil

  def partitionedLoad = List(1, 2, 4, 8, 10)
  def highContention = 10
  def messageSizes = List(20,1024,1024*256)

  override protected def beforeEach() = {
    totalProducerRate = new MetricAggregator().name("Aggregate Producer Rate").unit("items")
    totalConsumerRate = new MetricAggregator().name("Aggregate Consumer Rate").unit("items")
    brokers.clear
    producers.clear
    consumers.clear
    stopping.set(false)
    rcvBroker=null
    sendBroker=null
    producerCount = 0
    consumerCount = 0
    destCount = 0
    totalMessageSent = 0
    totalMessageReceived = 0
  }

  override protected def beforeAll(configMap: Map[String, Any]) = {
    super.beforeAll(configMap)
    if (TCP) {
      sendBrokerBindURI = "tcp://localhost:10000"
      receiveBrokerBindURI = "tcp://localhost:20000"

      sendBrokerConnectURI = "tcp://localhost:10000?protocol=" + getRemoteProtocolName()
      receiveBrokerConnectURI = "tcp://localhost:20000?protocol=" + getRemoteProtocolName()
    } else {
      sendBrokerConnectURI = "pipe://SendBroker"
      receiveBrokerConnectURI = "pipe://ReceiveBroker"

      sendBrokerBindURI = sendBrokerConnectURI
      receiveBrokerBindURI = receiveBrokerConnectURI
    }
  }

  def reportResourceTemplate:URL

  def reportTargetName = "perf-"+getClass.getName+".html"

  override protected def afterAll() = {
    val basedir = new File(System.getProperty("user.home", "."))
    val htmlFile = new File(basedir, reportTargetName)

    val report_parser = """(?s)(.*// DATA-START\r?\n)(.*)(// DATA-END.*<!-- DESCRIPTION-START -->)(.*)(<!-- DESCRIPTION-END -->.*)""".r



    // Load the previous dataset if the file exists
    var report_data = ""
    if( htmlFile.exists ) {
      IOHelper.readText(htmlFile) match {
        case report_parser(_, data, _, _, _) =>
          report_data = data.stripLineEnd
        case _ =>
          println("could not parse existing report file: "+htmlFile)
          val backup: File = new File(htmlFile.getParentFile, htmlFile.getName + ".bak")
          println("backing up to: "+backup)
          IOHelper.copyFile(htmlFile, backup )
      }
    }

    // Load the report template and parse it..
    val template = IOHelper.readText(reportResourceTemplate.openStream)
    template match {
      case report_parser(report_header, _, report_mid, _, report_footer) =>
        var notes = System.getProperty("notes")
        if( notes==null ) {
          val version = new String(ProcessSupport.system("git", "rev-list", "--max-count=1", "HEAD").toByteArray).trim
          notes = "commit "+version
        }

        if( !report_data.isEmpty ) {
          report_data += ",\n"
        }
        report_data += "            ['"+jsescape(notes)+"', "+samples.map(x=>String.format("%.2f",x._2)).mkString(", ")+"]\n"
        IOHelper.writeText(htmlFile, report_header+report_data+report_mid+description+report_footer)
      case _ =>
        println("could not parse template report file")
    }

    println("Updated: "+htmlFile)

    if( DUMP_REPORT_COLS ) {
      samples.map(_._1).foreach{x=>
        println("          data.addColumn('number', '"+x+"')")
      }
    }
  }

  def description = ""

  def jsescape(value:String) = {
    var rc = ""
    value.foreach{ c=>
      c match {
        case '\n'=> rc+="\\n"
        case '\r'=> rc+="\\r"
        case '\t'=> rc+="\\t"
        case '\''=> rc+="\\\'"
        case '\"'=> rc+="\\\""
        case _ => rc+=c
      }
    }
    rc
  }



  protected def createConsumer(): RemoteConsumer
  protected def createProducer(): RemoteProducer

  def getBrokerProtocolName() = "multi"
  def getRemoteProtocolName(): String

  def createBrokerConfig(name: String, bindURI: String, connectUri: String): BrokerDTO = {

    val config = Broker.defaultConfig
    val connector = config.connectors.get(0)
    connector.bind = bindURI
    connector.advertise = connectUri
    connector.protocol = getBrokerProtocolName

    val host = config.virtual_hosts.get(0)
    host.purge_on_startup = PURGE_STORE
    config
  }

  def createConnections() = {

    if (MULTI_BROKER) {
      sendBroker = new Broker()
      sendBroker.config = createBrokerConfig("SendBroker", sendBrokerBindURI, sendBrokerConnectURI)
      rcvBroker = new Broker()
      rcvBroker.config = createBrokerConfig("RcvBroker", receiveBrokerBindURI, receiveBrokerConnectURI)
      brokers += (sendBroker)
      brokers += (rcvBroker)
    } else {
      sendBroker = new Broker()
      rcvBroker = sendBroker
      sendBroker.config = createBrokerConfig("Broker", sendBrokerBindURI, sendBrokerConnectURI)
      brokers += (sendBroker)
    }

    startBrokers()

    var dests = new Array[Destination](destCount)

    for (i <- 0 until destCount) {
      val domain = if (PTP) {Router.QUEUE_DOMAIN} else {Router.TOPIC_DOMAIN}
      val name = new AsciiBuffer("dest" + (i + 1))
      var bean = new SingleDestination(domain, name)
      dests(i) = bean
//        if (PTP) {
//          sendBroker.defaultVirtualHost.createQueue(dests(i))
//          if (MULTI_BROKER) {
//            rcvBroker.defaultVirtualHost.createQueue(dests(i))
//          }
//        }
    }

    for (i <- 0 until producerCount) {
      var destination = dests(i % destCount)
      var producer = _createProducer(i, destination)
      producer.persistent = PERSISTENT
      producers += (producer)
    }

    for (i <- 0 until consumerCount) {
      var destination = dests(i % destCount)
      var consumer = _createConsumer(i, destination)
      consumer.persistent = PERSISTENT
      consumer.durable = DURABLE
      consumers += (consumer)
    }

    // Create MultiBroker connections:
    // if (multibroker) {
    // Pipe<Message> pipe = new Pipe<Message>()
    // sendBroker.createBrokerConnection(rcvBroker, pipe)
    // rcvBroker.createBrokerConnection(sendBroker, pipe.connect())
    // }
  }

  def _createConsumer(i: Int, destination: Destination): RemoteConsumer = {

    var consumer = createConsumer()
    consumer.stopping = stopping

    consumer.uri = connectUri(rcvBroker)
    consumer.destination = destination
    consumer.name = "Consumer:" + (i + 1)
    consumer.rateAggregator = totalConsumerRate
    consumer.init
    
    return consumer
  }

  def connectUri(broker:Broker) = {
    broker.config.connectors.get(0).advertise
  }


  def _createProducer(id: Int, destination: Destination): RemoteProducer = {
    var producer = createProducer()
    producer.stopping = stopping

    producer.uri = connectUri(sendBroker)
    producer.producerId = id + 1
    producer.name = "Producer:" + (id + 1)
    producer.destination = destination
    producer.messageIdGenerator = msgIdGenerator
    producer.rateAggregator = totalProducerRate
    producer.payloadSize = MESSAGE_SIZE
    producer.init
    producer
  }

  def stopServices() = {
    println("waiting for services to stop")
    stopping.set(true)
    var tracker = new LoggingTracker("broker shutdown")
    for (broker <- brokers) {
      tracker.stop(broker)
    }
    tracker.await
    tracker = new LoggingTracker("producer shutdown")
    for (connection <- producers) {
      tracker.stop(connection)
    }
    tracker.await
    tracker = new LoggingTracker("consumer shutdown")
    for (connection <- consumers) {
      tracker.stop(connection)
    }
    tracker.await
  }

  def startBrokers() = {
    val tracker = new LoggingTracker("test broker startup")
    for (broker <- brokers) {
      tracker.start(broker)
    }
    tracker.await
  }


  def startClients() = {
    var tracker = new LoggingTracker("test consumer startup")
    for (connection <- consumers) {
      tracker.start(connection)
    }
    tracker.await
    // let the consumers drain the destination for a bit...
    Thread.sleep(1000)
    tracker = new LoggingTracker("test producer startup")
    for (connection <- producers) {
      tracker.start(connection)
    }
    tracker.await
  }

  def fixed_sampling = true
  def keep_sampling = false

  def reportRates() = {

    case class Summary(producer:java.lang.Float, pdev:java.lang.Float, consumer:java.lang.Float, cdev:java.lang.Float)
    var best = 0
    import scala.collection.mutable.ArrayBuffer
    val sample_rates = new ArrayBuffer[Summary]()

    def fillRateSummary(i: Int): Unit = {
      val p = new Period()
      Thread.sleep(SAMPLE_PERIOD)
      if (producerCount > 0) {
        trace(totalProducerRate.getRateSummary(p))
      }
      if (consumerCount > 0) {
        trace(totalConsumerRate.getRateSummary(p))
      }

      sample_rates += Summary(totalProducerRate.total(p), totalProducerRate.deviation, totalConsumerRate.total(p), totalConsumerRate.deviation)

      val current_sum = sample_rates(i).producer.longValue + sample_rates(i).consumer.longValue
      val best_sum = sample_rates(best).producer.longValue + sample_rates(best).consumer.longValue
      if (current_sum > best_sum) {
        best = i
      }

      totalMessageSent += totalProducerRate.reset()
      totalMessageReceived += totalConsumerRate.reset()
    }

    // either we want to do x number of samples or we want to keep sampling while some condition is true.
    if ( fixed_sampling ) {

      // Do 1 period of warm up that's not counted...
      println("Warming up...")
      Thread.sleep(SAMPLE_PERIOD)
      totalMessageSent +=  totalProducerRate.reset()
      totalMessageSent +=  totalConsumerRate.reset()

      println("Sampling rates")
      for (i <- 0 until PERFORMANCE_SAMPLES) {
        fillRateSummary(i)
      }
    } else {
      println("Sampling rates")
      var i = 0
      while( keep_sampling ) {
        fillRateSummary(i)
        i += 1
      }
    }

    if( producerCount > 0 ) {
      samples = samples ::: ( testName+" producer", sample_rates(best).producer ) :: Nil
      if( producerCount > 1 ) {
        samples = samples ::: ( testName+" producer sd", sample_rates(best).pdev ) :: Nil
      }
    }
    if( consumerCount > 0 ) {
      samples = samples ::: ( testName+" consumer", sample_rates(best).consumer ) :: Nil
      if( consumerCount > 1 ) {
        samples = samples ::: ( testName+" consumer sd", sample_rates(best).cdev ) :: Nil
      }
    }
  }


}
abstract class RemoteConnection extends Connection {
  var uri: String = null
  var name:String = null

  val rate = new MetricCounter()
  var rateAggregator: MetricAggregator = null

  var stopping:AtomicBoolean = null
  var destination: Destination = null

  var messageCount = 0

  def init = {
    if( rate.getName == null ) {
      rate.name(name + " Rate")
    }
    rateAggregator.add(rate)
  }

  var callbackWhenConnected:Runnable = null

  override protected def _start(onComplete:Runnable) = {
    callbackWhenConnected = onComplete
    transport = TransportFactory.connect(uri)
    super._start(^{ })
  }

  override def onTransportConnected() = {
    onConnected()
    transport.resumeRead
    callbackWhenConnected.run
    callbackWhenConnected = null
  }

  protected def onConnected()

  override def onTransportFailure(error: IOException) = {
    if (!stopped) {
      if(stopping.get()) {
        transport.stop
      } else {
        onFailure(error)
        if( callbackWhenConnected!=null ) {
          warn("connect attempt failed. will retry connection..")
          dispatchQueue.dispatchAfter(50, TimeUnit.MILLISECONDS, ^{
            if(stopping.get()) {
              callbackWhenConnected.run
            } else {
              // try to connect again...
              transport = TransportFactory.connect(uri)
              super._start(^{ })
            }
          })
        }
      }
    }
  }

  protected def doStop()

  protected def incrementMessageCount() = {
    messageCount = messageCount + 1
  }

}

abstract class RemoteConsumer extends RemoteConnection {
  var thinkTime: Long = 0
  var selector: String = null
  var durable = false
  var persistent = false
}


abstract class RemoteProducer extends RemoteConnection {

  var messageIdGenerator: AtomicLong = null
  var priority = 0
  var persistent = false
  var priorityMod = 0
  var counter = 0
  var producerId = 0
  var property: String = null
  var next: Delivery = null
  var thinkTime: Long = 0

  var filler: String = null
  var payloadSize = 20

  override def init = {
    super.init

    if (payloadSize > 0) {
      var sb = new StringBuilder(payloadSize)
      for (i <- 0 until payloadSize) {
        sb.append(('a' + (i % 26)).toChar)
      }
      filler = sb.toString()
    }
  }

  def createPayload(): String = {
    if (payloadSize >= 0) {
      var sb = new StringBuilder(payloadSize)
      sb.append(name)
      sb.append(':')
      counter += 1
      sb.append(counter)
      sb.append(':')
      var length = sb.length
      if (length <= payloadSize) {
        sb.append(filler.subSequence(0, payloadSize - length))
        return sb.toString()
      } else {
        return sb.substring(0, payloadSize)
      }
    } else {
      counter += 1
      return name + ":" + (counter)
    }
  }

}