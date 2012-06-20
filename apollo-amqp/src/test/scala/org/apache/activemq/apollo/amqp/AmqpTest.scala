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
package org.apache.activemq.apollo.amqp

import org.scalatest.matchers.ShouldMatchers
import org.scalatest.BeforeAndAfterEach
import org.apache.activemq.apollo.broker._
import java.io.FileInputStream
import org.apache.activemq.apollo.util.FileSupport._
import org.fusesource.amqp.codec.AMQPProtocolCodec
import com.swiftmq.amqp.v100.client.ExceptionListener
import java.lang.Exception

class AmqpTestSupport extends BrokerFunSuiteSupport with ShouldMatchers with BeforeAndAfterEach {

  override def broker_config_uri = "xml:classpath:apollo-amqp.xml"

//  var client = new AmqpClient
//  var clients = List[AmqpClient]()
//
//  override protected def afterEach() = {
//    super.afterEach
//    clients.foreach(_.close)
//    clients = Nil
//  }
//
//  def connect_request(version:String, c: AmqpClient, headers:String="", connector:String=null) = {
//    val p = connector_port(connector).getOrElse(port)
//    c.open("localhost", p)
//    version match {
//      case "1.0"=>
//        c.write(
//          "CONNECT\n" +
//          headers +
//          "\n")
//      case "1.1"=>
//        c.write(
//          "CONNECT\n" +
//          "accept-version:1.1\n" +
//          "host:localhost\n" +
//          headers +
//          "\n")
//      case x=> throw new RuntimeException("invalid version: %f".format(x))
//    }
//    clients ::= c
//    c.receive()
//  }
//
//  def connect(version:String, c: AmqpClient = client, headers:String="", connector:String=null) = {
//    val frame = connect_request(version, c, headers, connector)
//    frame should startWith("CONNECTED\n")
//    frame should include regex("""session:.+?\n""")
//    frame should include("version:"+version+"\n")
//    c
//  }
//
//  val receipt_counter = new AtomicLong()
//
//  def sync_send(dest:String, body:Any, headers:String="", c:AmqpClient = client) = {
//    val rid = receipt_counter.incrementAndGet()
//    c.write(
//      "SEND\n" +
//      "destination:"+dest+"\n" +
//      "receipt:"+rid+"\n" +
//      headers+
//      "\n" +
//      body)
//    wait_for_receipt(""+rid, c)
//  }
//
//  def async_send(dest:String, body:Any, headers:String="", c: AmqpClient = client) = {
//    c.write(
//      "SEND\n" +
//      "destination:"+dest+"\n" +
//      headers+
//      "\n" +
//      body)
//  }
//
//  def subscribe(id:String, dest:String, mode:String="auto", persistent:Boolean=false, headers:String="", sync:Boolean=true, c: AmqpClient = client) = {
//    val rid = receipt_counter.incrementAndGet()
//    c.write(
//      "SUBSCRIBE\n" +
//      "destination:"+dest+"\n" +
//      "id:"+id+"\n" +
//      (if(persistent) "persistent:true\n" else "")+
//      "ack:"+mode+"\n"+
//      (if(sync) "receipt:"+rid+"\n" else "") +
//      headers+
//      "\n")
//    if(sync) {
//      wait_for_receipt(""+rid, c)
//    }
//  }
//
//  def unsubscribe(id:String, headers:String="", c: AmqpClient=client) = {
//    val rid = receipt_counter.incrementAndGet()
//    client.write(
//      "UNSUBSCRIBE\n" +
//      "id:"+id+"\n" +
//      "receipt:"+rid+"\n" +
//      headers+
//      "\n")
//    wait_for_receipt(""+rid, c)
//  }
//
//  def assert_received(body:Any, sub:String=null, c: AmqpClient=client):(Boolean)=>Unit = {
//    val frame = c.receive()
//    frame should startWith("MESSAGE\n")
//    if(sub!=null) {
//      frame should include ("subscription:"+sub+"\n")
//    }
//    body match {
//      case null =>
//      case body:scala.util.matching.Regex => frame should endWith regex(body)
//      case body => frame should endWith("\n\n"+body)
//    }
//    // return a func that can ack the message.
//    (ack:Boolean)=> {
//      val sub_regex = """(?s).*\nsubscription:([^\n]+)\n.*""".r
//      val msgid_regex = """(?s).*\nmessage-id:([^\n]+)\n.*""".r
//      val sub_regex(sub) = frame
//      val msgid_regex(msgid) = frame
//      c.write(
//        (if(ack) "ACK\n" else "NACK\n") +
//        "subscription:"+sub+"\n" +
//        "message-id:"+msgid+"\n" +
//        "\n")
//    }
//  }
//
//  def wait_for_receipt(id:String, c: AmqpClient = client, discard_others:Boolean=false): Unit = {
//    if( !discard_others ) {
//      val frame = c.receive()
//      frame should startWith("RECEIPT\n")
//      frame should include("receipt-id:"+id+"\n")
//    } else {
//      while(true) {
//        val frame = c.receive()
//        if( frame.startsWith("RECEIPT\n") && frame.indexOf("receipt-id:"+id+"\n")>=0 ) {
//          return
//        }
//      }
//    }
//  }
}

import com.swiftmq.amqp.AMQPContext
import com.swiftmq.amqp.v100.client.Connection
import com.swiftmq.amqp.v100.client.QoS
import com.swiftmq.amqp.v100.generated.messaging.message_format.AmqpValue
import com.swiftmq.amqp.v100.types.AMQPString

object PrintAMQPStream {
  def main(args: Array[String]) {
    for( arg <- args ) {
      println("--------------------------------------------------------")
      println(" File: "+arg)
      println("--------------------------------------------------------")
      using(new FileInputStream(arg)) { is =>
        val codec = new AMQPProtocolCodec
        codec.setReadableByteChannel(is.getChannel)
  //      codec.skipProtocolHeader()

        var pos = 0L
        var frame = codec.read()
        var counter = 0
        while( frame !=null ) {
          var next_pos = codec.getReadCounter - codec.getReadBytesPendingDecode
          counter += 1
          println("@"+pos+" "+frame)
          pos = next_pos;
          frame = try {
            codec.read()
          } catch {
            case e:java.io.EOFException => null
          }
        }
      }
    }
  }
}

class AmqpTest extends AmqpTestSupport {

  test("broker") {

//    val port = 5672
//    val queue = "testqueue"

    val queue = "/queue/testqueue"

    val nMsgs = 1
    val qos = QoS.AT_MOST_ONCE
    val ctx = new AMQPContext(AMQPContext.CLIENT);

    try {

      val connection = new Connection(ctx, "127.0.0.1", port, false)
      connection.setContainerId("client")
      connection.setIdleTimeout(-1)
      connection.setMaxFrameSize(1024*4)
      connection.setExceptionListener(new ExceptionListener(){
        def onException(e: Exception) {
          e.printStackTrace();
        }
      })
      connection.connect;
      {
        var data = "x" * 10 // 1024*20

        var session = connection.createSession(10, 10)
        var p = {
          session.createProducer(queue, qos)
        }
        for (i <- 0 until nMsgs) {
          var msg = new com.swiftmq.amqp.v100.messaging.AMQPMessage
          var s = "Message #" + (i + 1)
          println("Sending " + s)
          msg.setAmqpValue(new AmqpValue(new AMQPString(s+", data: "+data)))
          p.send(msg)
        }
        p.close()
        session.close()
      }
      {
        var session = connection.createSession(10, 10)
        val c = session.createConsumer(queue, 100, qos, true, null);

        // Receive messages non-transacted
        for (i <- 0 until nMsgs)
        {
          val msg = c.receive();
          if (msg == null)

          msg.getAmqpValue().getValue match {
            case value:AMQPString =>
              println("Received: " + value.getValue());
          }
          if (!msg.isSettled())
            msg.accept();
        }
        c.close()
        session.close()
      }
      connection.close()
    } catch {
      case e: Exception => {
        e.printStackTrace
      }
    }

  }
}