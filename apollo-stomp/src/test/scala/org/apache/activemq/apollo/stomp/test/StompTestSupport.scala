/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.apollo.stomp.test

import org.scalatest.matchers.ShouldMatchers
import org.scalatest._
import java.lang.String
import java.util.concurrent.atomic.AtomicLong
import org.apache.activemq.apollo.broker._

class StompTestSupport extends BrokerFunSuiteSupport with ShouldMatchers with BeforeAndAfterEach {

  override def broker_config_uri = "xml:classpath:apollo-stomp.xml"

  var client = new StompClient
  var clients = List[StompClient]()

  override protected def afterEach() = {
    super.afterEach
    clients.foreach(_.close)
    clients = Nil
  }

  def connect_request(version: String, c: StompClient, headers: String = "", connector: String = null) = {
    val p = connector_port(connector).getOrElse(port)
    c.open("localhost", p)
    version match {
      case "1.0" =>
        c.write(
          "CONNECT\n" +
                  headers +
                  "\n")
      case "1.1" =>
        c.write(
          "CONNECT\n" +
                  "accept-version:1.1\n" +
                  "host:localhost\n" +
                  headers +
                  "\n")
      case x => throw new RuntimeException("invalid version: %f".format(x))
    }
    clients ::= c
    c.receive()
  }

  def connect(version: String, c: StompClient = client, headers: String = "", connector: String = null) = {
    val frame = connect_request(version, c, headers, connector)
    frame should startWith("CONNECTED\n")
    frame should include regex ("""session:.+?\n""")
    frame should include("version:" + version + "\n")
    c
  }

  def disconnect(c: StompClient = client) = {
    val rid = receipt_counter.incrementAndGet()
    client.write(
      "DISCONNECT\n" +
              "receipt:" + rid + "\n" +
              "\n")
    wait_for_receipt("" + rid, c)
    close(c)
  }

  def close(c: StompClient = client) = c.close()

  val receipt_counter = new AtomicLong()

  def sync_send(dest: String, body: Any, headers: String = "", c: StompClient = client) = {
    val rid = receipt_counter.incrementAndGet()
    c.write(
      "SEND\n" +
              "destination:" + dest + "\n" +
              "receipt:" + rid + "\n" +
              headers +
              "\n" +
              body)
    wait_for_receipt("" + rid, c)
  }

  def async_send(dest: String, body: Any, headers: String = "", c: StompClient = client) = {
    c.write(
      "SEND\n" +
              "destination:" + dest + "\n" +
              headers +
              "\n" +
              body)
  }

  def subscribe(id: String, dest: String, mode: String = "auto", persistent: Boolean = false, headers: String = "", sync: Boolean = true, c: StompClient = client) = {
    val rid = receipt_counter.incrementAndGet()
    c.write(
      "SUBSCRIBE\n" +
              "destination:" + dest + "\n" +
              "id:" + id + "\n" +
              (if (persistent) "persistent:true\n" else "") +
              "ack:" + mode + "\n" +
              (if (sync) "receipt:" + rid + "\n" else "") +
              headers +
              "\n")
    if (sync) {
      wait_for_receipt("" + rid, c)
    }
  }

  def unsubscribe(id: String, headers: String = "", c: StompClient = client) = {
    val rid = receipt_counter.incrementAndGet()
    c.write(
      "UNSUBSCRIBE\n" +
              "id:" + id + "\n" +
              "receipt:" + rid + "\n" +
              headers +
              "\n")
    wait_for_receipt("" + rid, c)
  }

  def assert_received(body: Any, sub: String = null, c: StompClient = client, txid: String = null): (Boolean) => Unit = {
    val frame = c.receive()
    frame should startWith("MESSAGE\n")
    if (sub != null) {
      frame should include("subscription:" + sub + "\n")
    }
    body match {
      case null =>
      case body: scala.util.matching.Regex => frame should endWith regex (body)
      case body => frame should endWith("\n\n" + body)
    }
    // return a func that can ack the message.
    (ack: Boolean) => {
      val sub_regex = """(?s).*\nsubscription:([^\n]+)\n.*""".r
      val msgid_regex = """(?s).*\nmessage-id:([^\n]+)\n.*""".r
      val sub_regex(sub) = frame
      val msgid_regex(msgid) = frame
      c.write(
        (if (ack) "ACK\n" else "NACK\n") +
                "subscription:" + sub + "\n" +
                "message-id:" + msgid + "\n" +
                (if (txid != null) "transaction:" + txid + "\n" else "") +

                "\n")
    }
  }

  def wait_for_receipt(id: String, c: StompClient = client, discard_others: Boolean = false): Unit = {
    if (!discard_others) {
      val frame = c.receive()
      frame should startWith("RECEIPT\n")
      frame should include("receipt-id:" + id + "\n")
    } else {
      while (true) {
        val frame = c.receive()
        if (frame.startsWith("RECEIPT\n") && frame.indexOf("receipt-id:" + id + "\n") >= 0) {
          return
        }
      }
    }
  }
}
