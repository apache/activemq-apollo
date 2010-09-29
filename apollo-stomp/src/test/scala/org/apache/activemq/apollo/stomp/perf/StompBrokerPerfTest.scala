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
package org.apache.activemq.apollo.stomp.perf

import _root_.java.util.concurrent.TimeUnit
import _root_.org.apache.activemq.apollo.broker._
import _root_.org.apache.activemq.apollo.broker.perf._
import _root_.org.apache.activemq.apollo.stomp._
import _root_.org.apache.activemq.apollo.util._

import _root_.org.fusesource.hawtbuf._
import collection.mutable.{ListBuffer, HashMap}

import AsciiBuffer._
import Stomp._
import _root_.org.apache.activemq.apollo.stomp.StompFrame
import _root_.org.fusesource.hawtdispatch.ScalaDispatch._
import java.io.File
import org.apache.activemq.apollo.dto.{BrokerDTO, HawtDBStoreDTO}
import org.apache.activemq.apollo.store.bdb.dto.BDBStoreDTO


class StompBrokerPerfTest extends BaseBrokerPerfSupport {

  override def description = "Using the STOMP protocol over TCP"

  override def createProducer() = new StompRemoteProducer()

  override def createConsumer() = new StompRemoteConsumer()

  override def getRemoteProtocolName() = "stomp"

}

class StompPersistentBrokerPerfTest extends BasePersistentBrokerPerfSupport {

  override def description = "Using the STOMP protocol over TCP with no store."

  override def createProducer() = new StompRemoteProducer()

  override def createConsumer() = new StompRemoteConsumer()

  override def getRemoteProtocolName() = "stomp"

}

class StompHawtDBPersistentBrokerPerfTest extends BasePersistentBrokerPerfSupport {
  
  override def description = "Using the STOMP protocol over TCP persisting to the HawtDB store."

  println(getClass.getClassLoader.getResource("log4j.properties"))

  override def createProducer() = new StompRemoteProducer()

  override def createConsumer() = new StompRemoteConsumer()
 
  override def getRemoteProtocolName() = "stomp"

  override def createBrokerConfig(name: String, bindURI: String, connectUri: String): BrokerDTO = {
    val rc = super.createBrokerConfig(name, bindURI, connectUri)

    val store = new HawtDBStoreDTO
    store.directory = new File(new File(testDataDir, getClass.getName), name)

    rc.virtual_hosts.get(0).store = store
    rc
  }

}

class StompBDBPersistentBrokerPerfTest extends BasePersistentBrokerPerfSupport {

  override def description = "Using the STOMP protocol over TCP persisting to the BerkleyDB store."

  println(getClass.getClassLoader.getResource("log4j.properties"))

  override def createProducer() = new StompRemoteProducer()

  override def createConsumer() = new StompRemoteConsumer()

  override def getRemoteProtocolName() = "stomp"

  override def createBrokerConfig(name: String, bindURI: String, connectUri: String): BrokerDTO = {
    val rc = super.createBrokerConfig(name, bindURI, connectUri)

    val store = new BDBStoreDTO
    store.directory = new File(new File(testDataDir, getClass.getName), name)

    rc.virtual_hosts.get(0).store = store
    rc
  }

}


class StompRemoteConsumer extends RemoteConsumer with Logging {
  var outboundSink: OverflowSink[StompFrame] = null

  def onConnected() = {
    outboundSink = new OverflowSink[StompFrame](MapSink(transportSink){ x=>x })
    outboundSink.refiller = ^{}

    val stompDestination = if (destination.getDomain() == Router.QUEUE_DOMAIN) {
      ascii("/queue/" + destination.getName().toString());
    } else {
      ascii("/topic/" + destination.getName().toString());
    }

    var frame = StompFrame(Stomp.Commands.CONNECT);
    outboundSink.offer(frame);

    var headers: List[(AsciiBuffer, AsciiBuffer)] = Nil
    headers ::= (Stomp.Headers.Subscribe.DESTINATION, stompDestination)
    headers ::= (Stomp.Headers.Subscribe.ID, ascii("stomp-sub-" + name))

    if( persistent ) {
      headers ::= (Stomp.Headers.Subscribe.ACK_MODE, Stomp.Headers.Subscribe.AckModeValues.CLIENT)
    }

    frame = StompFrame(Stomp.Commands.SUBSCRIBE, headers);
    outboundSink.offer(frame);
  }

  override def onTransportCommand(command: Object) = {
    var frame = command.asInstanceOf[StompFrame]
    frame match {
      case StompFrame(Responses.CONNECTED, headers, _, _) =>
      case StompFrame(Responses.MESSAGE, headers, content, _) =>
          messageReceived();

          // we client ack if persistent messages are being used.
          if( persistent ) {
            var rc = List((Stomp.Headers.Ack.MESSAGE_ID, frame.header(Stomp.Headers.Message.MESSAGE_ID)))
            outboundSink.offer(StompFrame(Stomp.Commands.ACK, rc));
          }
          messageCount = messageCount + 1
          if ( messageCount % 10000 == 0 ) {
            trace("Received message count : " + messageCount)
          }
          if (maxMessages > 0 && messageCount >= maxMessages - 1) {
            stop()
          }

      case StompFrame(Responses.ERROR, headers, content, _) =>
        onFailure(new Exception("Server reported an error: " + frame.content));
      case _ =>
        onFailure(new Exception("Unexpected stomp command: " + frame.action));
    }
  }

  protected def messageReceived() {
      if (thinkTime > 0) {
        transport.suspendRead
        dispatchQueue.dispatchAfter(thinkTime, TimeUnit.MILLISECONDS, ^ {
          rate.increment();
          if (!stopped) {
            transport.resumeRead
          }
        })
      } else {
        rate.increment();
      }
  }

}

class StompRemoteProducer extends RemoteProducer with Logging {
  var outboundSink: OverflowSink[StompFrame] = null
  var stompDestination: AsciiBuffer = null
  var frame:StompFrame = null

  def send_next: Unit = {
      var headers: List[(AsciiBuffer, AsciiBuffer)] = Nil
      headers ::= (Stomp.Headers.Send.DESTINATION, stompDestination);
      if (property != null) {
        headers ::= (ascii(property), ascii(property));
      }
      if( persistent ) {
        headers ::= ((Stomp.Headers.RECEIPT_REQUESTED, ascii("x")));
      }
      //    var p = this.priority;
      //    if (priorityMod > 0) {
      //        p = if ((counter % priorityMod) == 0) { 0 } else { priority }
      //    }

      var content = ascii(createPayload());
      frame = StompFrame(Stomp.Commands.SEND, headers, BufferContent(content))
      messageCount = messageCount + 1
      if ( messageCount % 10000 == 0 ) {
        trace("Sent message count : " + messageCount)
      }
      if (maxMessages > 0 && messageCount >= maxMessages) {
        stop()
      }    
      drain()
  }

  def drain() = {
    if( frame!=null ) {
      if( !outboundSink.full ) {
        outboundSink.offer(frame)
        frame = null
        rate.increment();
        val task = ^ {
          if (!stopped) {
            send_next
          }
        }

        if( !persistent ) {
          // if we are not going to wait for an ack back from the server,
          // then jut send the next one...
          if (thinkTime > 0) {
            dispatchQueue.dispatchAfter(thinkTime, TimeUnit.MILLISECONDS, task)
          } else {
            dispatchQueue << task
          }
        }
      }
    }
  }

  override def onConnected() = {
    outboundSink = new OverflowSink[StompFrame](MapSink(transportSink){ x=>x })
    outboundSink.refiller = ^ { drain }

    if (destination.getDomain() == Router.QUEUE_DOMAIN) {
      stompDestination = ascii("/queue/" + destination.getName().toString());
    } else {
      stompDestination = ascii("/topic/" + destination.getName().toString());
    }
    outboundSink.offer(StompFrame(Stomp.Commands.CONNECT));
    send_next
  }

  override def onTransportCommand(command: Object) = {
    var frame = command.asInstanceOf[StompFrame]
    frame match {
      case StompFrame(Responses.RECEIPT, headers, _, _) =>
        assert( persistent )
        // we got the ack for the previous message we sent.. now send the next one.
        send_next

      case StompFrame(Responses.CONNECTED, headers, _, _) =>
      case StompFrame(Responses.ERROR, headers, content, _) =>
        onFailure(new Exception("Server reported an error: " + frame.content.utf8));
      case _ =>
        onFailure(new Exception("Unexpected stomp command: " + frame.action));
    }
  }

}

