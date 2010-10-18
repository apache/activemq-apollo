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

import AsciiBuffer._
import Stomp._
import _root_.org.apache.activemq.apollo.stomp.StompFrame
import _root_.org.fusesource.hawtdispatch.ScalaDispatch._


class StompRemoteConsumer extends RemoteConsumer {
  var outboundSink: OverflowSink[StompFrame] = null

  def onConnected() = {
    outboundSink = new OverflowSink[StompFrame](MapSink(transportSink) {x => x})
    outboundSink.refiller = ^ {}

    val stompDestination = if (destination.getDomain() == Router.QUEUE_DOMAIN) {
      ascii("/queue/" + destination.getName().toString());
    } else {
      ascii("/topic/" + destination.getName().toString());
    }

    var frame = StompFrame(CONNECT);
    outboundSink.offer(frame);

    var headers: List[(AsciiBuffer, AsciiBuffer)] = Nil
    headers ::= (DESTINATION, stompDestination)
    headers ::= (ID, ascii("stomp-sub-" + name))

    if( persistent ) {
      headers ::= (ACK_MODE, CLIENT)
    }

    frame = StompFrame(SUBSCRIBE, headers);
    outboundSink.offer(frame);
  }

  override def onTransportCommand(command: Object) = {
    var frame = command.asInstanceOf[StompFrame]
    frame match {
      case StompFrame(CONNECTED, headers, _, _) =>
      case StompFrame(MESSAGE, headers, content, _) =>
          messageReceived();

          // we client ack if persistent messages are being used.
          if( persistent ) {
            var rc = List((MESSAGE_ID, frame.header(MESSAGE_ID)))
            outboundSink.offer(StompFrame(ACK, rc));
          }

      case StompFrame(ERROR, headers, content, _) =>
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
        rate.increment
      }
  }
}

class StompRemoteProducer extends RemoteProducer with Logging {
  var outboundSink: OverflowSink[StompFrame] = null
  var stompDestination: AsciiBuffer = null
  var frame: StompFrame = null

  def send_next: Unit = {
      var headers: List[(AsciiBuffer, AsciiBuffer)] = Nil
      headers ::= (DESTINATION, stompDestination);
      if (property != null) {
        headers ::= (ascii(property), ascii(property));
      }
      if( persistent ) {
        headers ::= ((RECEIPT_REQUESTED, ascii("x")));
      }
      //    var p = this.priority;
      //    if (priorityMod > 0) {
      //        p = if ((counter % priorityMod) == 0) { 0 } else { priority }
      //    }

      var content = ascii(createPayload());
      frame = StompFrame(SEND, headers, BufferContent(content))
      drain()
  }

  def drain() = {
    if (frame != null) {
      if (!outboundSink.full) {
        outboundSink.offer(frame)
        frame = null
        rate.increment
        val task = ^ {
          if (!stopped) {
            send_next
          }
        }

        if (!persistent) {
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
    outboundSink = new OverflowSink[StompFrame](MapSink(transportSink) {x => x})
    outboundSink.refiller = ^ {drain}

    if (destination.getDomain() == Router.QUEUE_DOMAIN) {
      stompDestination = ascii("/queue/" + destination.getName().toString());
    } else {
      stompDestination = ascii("/topic/" + destination.getName().toString());
    }
    outboundSink.offer(StompFrame(CONNECT));
    send_next
  }

  override def onTransportCommand(command: Object) = {
    var frame = command.asInstanceOf[StompFrame]
    frame match {
      case StompFrame(RECEIPT, headers, _, _) =>
        assert( persistent )
        // we got the ack for the previous message we sent.. now send the next one.
        send_next

      case StompFrame(CONNECTED, headers, _, _) =>
      case StompFrame(ERROR, headers, content, _) =>
        onFailure(new Exception("Server reported an error: " + frame.content.utf8));
      case _ =>
        onFailure(new Exception("Unexpected stomp command: " + frame.action));
    }
  }
}

trait Watchog extends RemoteConsumer {
  var messageCount = 0

  def watchdog(lastMessageCount: Int): Unit = {
    val seconds = 10
    dispatchQueue.dispatchAfter(seconds, TimeUnit.SECONDS, ^ {
      if (messageCount == lastMessageCount) {
        warn("Messages have stopped arriving after " + seconds + "s, stopping consumer")
        stop
      } else {
        watchdog(messageCount)
      }
    })
  }

  abstract override protected def messageReceived() = {
    super.messageReceived
    messageCount += 1
  }

  abstract override protected def onConnected() = {
    super.onConnected
    watchdog(messageCount)
  }

}

