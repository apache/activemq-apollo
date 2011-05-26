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
import collection.JavaConversions._
import java.lang.StringBuilder
import java.util.regex.Pattern

object DestinationParser {

  val OPENWIRE_PARSER = new DestinationParser();
  OPENWIRE_PARSER.path_separator = "."
  OPENWIRE_PARSER.any_child_wildcard = "*"
  OPENWIRE_PARSER.any_descendant_wildcard = ">"

  def create_destination(domain:String, parts:Array[String]):DestinationDTO = domain match {
    case LocalRouter.QUEUE_DOMAIN => new QueueDestinationDTO(parts)
    case LocalRouter.TOPIC_DOMAIN => new TopicDestinationDTO(parts)
    case _ => throw new Exception("Uknown destination domain: "+domain);
  }

}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class DestinationParser extends PathParser {
  import DestinationParser._

  var default_domain:String = null
  var queue_prefix = "queue:"
  var topic_prefix = "topic:"
  var temp_queue_prefix = "temp-queue:"
  var temp_topic_prefix = "temp-topic:"
  var destination_separator = ","

  def copy(other:DestinationParser) = {
    super.copy(other)
    default_domain = other.default_domain
    queue_prefix = other.queue_prefix
    topic_prefix = other.topic_prefix
    temp_queue_prefix = other.temp_queue_prefix
    temp_topic_prefix = other.temp_topic_prefix
    destination_separator = other.destination_separator
    this
  }

  def encode_destination(value: Array[DestinationDTO]): String = {
    if (value == null) {
      null
    } else {
      val rc = new StringBuilder
      value.foreach { dest =>
        if (rc.length() != 0 ) {
          assert( destination_separator!=null )
          rc.append(destination_separator)
        }
        dest match {
          case d:QueueDestinationDTO =>
            rc.append(queue_prefix)
          case d:TopicDestinationDTO =>
            rc.append(topic_prefix)
//          case Router.TEMP_QUEUE_DOMAIN =>
//            baos.write(temp_queue_prefix)
//          case Router.TEMP_TOPIC_DOMAIN =>
//            baos.write(temp_topic_prefix)
          case _ =>
            throw new Exception("Uknown destination type: "+dest.getClass);
        }
        rc.append(encode_path(dest.path.toIterable))

      }
      rc.toString
    }
  }

  /**
   * Parses a destination which may or may not be a composite.
   *
   * @param value
   * @param compositeSeparator
   * @return
   */
  def decode_destination(value: String): Array[DestinationDTO] = {
    if (value == null) {
      return null;
    }

    if (destination_separator!=null && value.contains(destination_separator)) {
      var rc = value.split(Pattern.quote(destination_separator));
      var dl = ListBuffer[DestinationDTO]()
      for (buffer <- rc) {
        val d = decode_destination(buffer)
        if (d == null) {
          return null;
        }
        dl += d(0)
      }
      return dl.toArray
    } else {

      if (queue_prefix != null && value.startsWith(queue_prefix)) {
        var name = value.substring(queue_prefix.length)
        return Array( create_destination(LocalRouter.QUEUE_DOMAIN, parts(name)) )
      } else if (topic_prefix != null && value.startsWith(topic_prefix)) {
        var name = value.substring(topic_prefix.length)
        return Array(create_destination(LocalRouter.TOPIC_DOMAIN, parts(name)))
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
        return Array(create_destination(default_domain, parts(value)))
      }
    }
  }
}

