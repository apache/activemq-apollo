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

package org.apache.activemq.apollo.broker.network

import org.fusesource.hawtdispatch._
import java.net.URI
import org.apache.activemq.apollo.util.{StateMachine, Log}
import org.fusesource.hawtbuf.AsciiBuffer
import org.fusesource.stomp.codec.StompFrame
import org.apache.activemq.apollo.broker.Broker
import java.util.Properties
import org.fusesource.stomp.client.{CallbackConnection, Stomp}
import java.util.concurrent.TimeUnit
import collection.mutable.HashMap


object StompBridgingStrategy extends Log {
}

class StompBridgingStrategy(val manager:NetworkManager) extends BridgingStrategy {
  import StompBridgingStrategy._
  import org.fusesource.stomp.client.Constants._
  import org.fusesource.hawtbuf.Buffer._
  def dispatch_queue = manager.dispatch_queue

  val bridges = HashMap[(String, String), Bridge]()

  def deploy(info:BridgeInfo) = {
    dispatch_queue.assertExecuting()
    val bridge = bridges.getOrElseUpdate((info.from, info.to), new Bridge(info.from, info.to))
    bridge.deploy(info.kind, info.dest)
  }


  def undeploy(info:BridgeInfo) = {
    dispatch_queue.assertExecuting()
    for( bridge <- bridges.get((info.from, info.to)) ) {
      bridge.undeploy(info.kind, info.dest)
    }
  }

  class Bridge(from:String, to:String) {
    val dispatch_queue = createQueue("bridge %s -> %s".format(from, to))

    val from_connection = ConnectionStateMachine(new URI(from))
    val to_connection = ConnectionStateMachine(new URI(to))

    from_connection.refiller = ^{

    }

    dispatch_queue {
      from_connection.connect
      to_connection.connect
    }

    case class ConnectionStateMachine(uri:URI) extends StateMachine {

      var id_counter = 0L
      var subscriptions = HashMap[AsciiBuffer, AsciiBuffer]()
      var pending_sends = HashMap[Long, (StompFrame, ()=>Unit)]()

      var refiller: Runnable = ^{ sys.error("refiller not set") }
      var receive_handler: (StompFrame)=>Boolean = frame => {
        info("dropping frame: %s", frame)
        true
      }


      def next_id = {
        val rc = id_counter
        id_counter += 1
        rc
      }

      def next_hex_id = {
        ascii(next_id.toHexString)
      }


      def init() = DisconnectedState()

      case class DisconnectedState() extends State {
        def connect = react {
          become(ConnectingState())
        }
      }

      case class ConnectingState() extends State {
        override def init() = {
          debug("Connecting bridge to %s", uri)
          val to_stomp = new Stomp()
          to_stomp.setDispatchQueue(dispatch_queue)
          to_stomp.setRemoteURI(uri)
          to_stomp.setLogin("admin")
          to_stomp.setPasscode("password")
          to_stomp.setBlockingExecutor(Broker.BLOCKABLE_THREAD_POOL)
          val headers = new Properties()
          headers.put("client-type", "apollo-bridge")
          to_stomp.setCustomHeaders(headers)

          to_stomp.connectCallback(new org.fusesource.stomp.client.Callback[CallbackConnection] {
            override def onSuccess(value: CallbackConnection) = react {
              become(ConnectedState(value))
            }
            override def onFailure(value: Throwable) = react {
              debug("Could not connect bridge to %s due to: ", uri, value)
              become(ReconnectDelayState(1000))
            }
          })
        }
      }

      case class ReconnectDelayState(delay:Long) extends State {
        override def init() = {
          debug("Will attempt a reconnect to %s in %d ms", uri, delay)
          dispatch_queue.after(delay, TimeUnit.MILLISECONDS) {
            react(become(ConnectingState()))
          }
        }
      }

      case class ConnectedState(connection:CallbackConnection) extends State {

        var closed  = false

        override def init() = {
          debug("Bridge connected to: %s", uri)
          connection.receive(new org.fusesource.stomp.client.Callback[StompFrame] {
            override def onSuccess(value: StompFrame) = {
              if( !receive_handler(value) ) {
                connection.suspend()
              }
            }
            override def onFailure(value: Throwable) = {
              failed(value)
            }
          })
          connection.refiller(refiller)

          // Reconnect any subscriptions.
          subscriptions.keySet.foreach(subscribe(_))
          // Re-send messages..
          pending_sends.values.foreach(x => do_send(x._1, x._2))

        }

        def do_send(frame:StompFrame, on_complete: ()=>Unit) = {
          connection.request(frame, new org.fusesource.stomp.client.Callback[StompFrame] {
            override def onSuccess(response: StompFrame) = on_complete()
            override def onFailure(value: Throwable) = failed(value)
          })
        }

        def failed(value: Throwable)= {
          debug("Bridge connection to %s failed due to: ", uri, value)
          close(ReconnectDelayState(1000))
        }

        def close(next: State) = {
          if( closed ) {
            become(next)
          } else {
            closed = true
            val pause = Pause(next)
            become(pause)
            debug("Closing connection to %s", uri)
            connection.close(^{
              debug("Closed connection to %s", uri)
              pause.continue
            })
          }
        }
      }

      def connect = react[DisconnectedState] { state => state.connect }
      def for_connection(func: (CallbackConnection)=>Unit) = react[ConnectedState] { state => func(state.connection) }
      def suspend = for_connection { connection => connection.suspend() }
      def resume = for_connection { connection => connection.resume() }

      def subscribe(destination:AsciiBuffer) = {
        val id = subscriptions.getOrElseUpdate(destination, next_hex_id)
        for_connection{ connection =>
          val frame = new StompFrame(SUBSCRIBE)
          frame.addHeader(ID, id)
          frame.addHeader(DESTINATION, destination)
          frame.addHeader(ACK_MODE, CLIENT)
          connection.send(frame, null)
        }
      }

      def unsubscribe(destination:AsciiBuffer) = {
        subscriptions.remove(destination) match {
          case Some(id) =>
            for_connection{ connection =>
              val frame = new StompFrame(UNSUBSCRIBE)
              frame.addHeader(ID, id)
              connection.send(frame, null)
            }
          case None =>
        }
      }

      def send(destination:StompFrame, on_complete: ()=>Unit) = {
        val id = next_id
        val cb = ()=>{
          pending_sends.remove(id)
          on_complete()
        }
        pending_sends.put(id, (destination, cb))
        react[ConnectedState] { state => state.do_send(destination, cb) }
      }
    }

    def deploy(kind:String, destination:String) = dispatch_queue {
      val destination_uri = ascii("/%s/%s".format(kind, destination))
      from_connection.subscribe(destination_uri)
    }

    def undeploy(kind:String, destination:String) = dispatch_queue {
      val destination_uri = ascii("/%s/%s".format(kind, destination))
      from_connection.unsubscribe(destination_uri)
    }

  }

}

