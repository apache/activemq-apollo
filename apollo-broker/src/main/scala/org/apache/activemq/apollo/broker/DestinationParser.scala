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

import _root_.org.fusesource.hawtbuf._
import BufferConversions._
import Buffer._
import org.apache.activemq.apollo.util.path.{Path, PathParser}
import scala.collection.mutable.ListBuffer
import org.apache.activemq.apollo.dto.{TopicDestinationDTO, QueueDestinationDTO, DestinationDTO}

object DestinationParser {

  val default = new DestinationParser

  def encode_path(value:Path) = default.toString(value)
  def decode_path(value:String) = default.parsePath(ascii(value))

  def encode_destination(value:Array[DestinationDTO]) = default.toString(value)
  def decode_destination(value:String) = default.parse(ascii(value))

  def create_destination(domain:AsciiBuffer, name:String) = {
    Array(domain match {
      case LocalRouter.QUEUE_DOMAIN => new QueueDestinationDTO(name)
      case LocalRouter.TOPIC_DOMAIN => new TopicDestinationDTO(name)
      case _ => throw new Exception("Uknown destination domain: "+domain);
    })
  }

}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class DestinationParser extends PathParser {
  import DestinationParser._

  var default_domain: AsciiBuffer = null
  var queue_prefix: AsciiBuffer = ascii("queue:")
  var topic_prefix: AsciiBuffer = ascii("topic:")
  var temp_queue_prefix: AsciiBuffer = ascii("temp-queue:")
  var temp_topic_prefix: AsciiBuffer = ascii("temp-topic:")
  var destination_separator: Option[Byte] = Some(','.toByte)

  def toBuffer(value: Array[DestinationDTO]): AsciiBuffer = {
    if (value == null) {
      null
    } else {
      val baos = new ByteArrayOutputStream
      val first = true
      value.foreach { d =>
        if (!first) {
          assert( destination_separator.isDefined )
          baos.write(destination_separator.get)
        }
        d match {
          case d:QueueDestinationDTO =>
            baos.write(queue_prefix)
          case d:TopicDestinationDTO =>
            baos.write(topic_prefix)
//          case Router.TEMP_QUEUE_DOMAIN =>
//            baos.write(temp_queue_prefix)
//          case Router.TEMP_TOPIC_DOMAIN =>
//            baos.write(temp_topic_prefix)
          case _ =>
            throw new Exception("Uknown destination type: "+d.getClass);
        }
        ascii(d.name).writeTo(baos)
      }
      baos.toBuffer.ascii
    }
  }

  def toString(value:Array[DestinationDTO]) = toBuffer(value).toString

  /**
   * Parses a destination which may or may not be a composite.
   *
   * @param value
   * @param compositeSeparator
   * @return
   */
  def parse(value: AsciiBuffer): Array[DestinationDTO] = {
    if (value == null) {
      return null;
    }

    if (destination_separator.isDefined && value.contains(destination_separator.get)) {
      var rc = value.split(destination_separator.get);
      var dl = ListBuffer[DestinationDTO]()
      for (buffer <- rc) {
        val d = parse(buffer)
        if (d == null) {
          return null;
        }
        dl += d(0)
      }
      return dl.toArray
    } else {

      if (queue_prefix != null && value.startsWith(queue_prefix)) {
        var name = value.slice(queue_prefix.length, value.length).ascii()
        return create_destination(LocalRouter.QUEUE_DOMAIN, name.toString)
      } else if (topic_prefix != null && value.startsWith(topic_prefix)) {
        var name = value.slice(topic_prefix.length, value.length).ascii()
        return create_destination(LocalRouter.TOPIC_DOMAIN, name.toString)
//      } else if (temp_queue_prefix != null && value.startsWith(temp_queue_prefix)) {
//        var name = value.slice(temp_queue_prefix.length, value.length).ascii()
//        return new DestinationDTO(LocalRouter.TEMP_QUEUE_DOMAIN, name.toString)
//      } else if (temp_topic_prefix != null && value.startsWith(temp_topic_prefix)) {
//        var name = value.slice(temp_topic_prefix.length, value.length).ascii()
//        return new DestinationDTO(LocalRouter.TEMP_TOPIC_DOMAIN, name.toString)
      } else {
        if (default_domain == null) {
          return null;
        }
        return create_destination(default_domain, value.toString)
      }
    }
  }
}

