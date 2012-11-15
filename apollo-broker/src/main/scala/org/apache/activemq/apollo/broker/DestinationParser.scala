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

import org.apache.activemq.apollo.util.path.PathParser
import scala.collection.mutable.ListBuffer
import java.lang.StringBuilder
import java.util.regex.Pattern
import scala.Array

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class DestinationParser extends PathParser {

  var queue_prefix = "queue:"
  var topic_prefix = "topic:"
  var dsub_prefix = "dsub:"
  var temp_queue_prefix = "temp-queue:"
  var temp_topic_prefix = "temp-topic:"
  var destination_separator = ","

  def copy(other:DestinationParser) = {
    super.copy(other)
    queue_prefix = other.queue_prefix
    topic_prefix = other.topic_prefix
    dsub_prefix = other.dsub_prefix
    temp_queue_prefix = other.temp_queue_prefix
    temp_topic_prefix = other.temp_topic_prefix
    destination_separator = other.destination_separator
    this
  }

  def encode_destination(addresses: Seq[_ <: DestinationAddress]): String = {
    if (addresses == null) {
      null
    } else {
      val rc = new StringBuilder
      addresses.foreach { address:DestinationAddress =>
        if (rc.length() != 0 ) {
          assert( destination_separator!=null )
          rc.append(destination_separator)
        }
        rc.append(encode_destination(address))
      }
      rc.toString
    }
  }

  def encode_destination(address: DestinationAddress): String = {
    if (address == null) {
      null
    } else {
      val rc = new StringBuilder
      address.domain match {
        case "temp-queue" => if( temp_queue_prefix!=null ) {
          rc.append(temp_queue_prefix)
        }
        case "queue" => if( queue_prefix!=null ) {
          rc.append(queue_prefix)
        }
        case "temp-topic" => if( temp_topic_prefix!=null ) {
          rc.append(temp_topic_prefix)
        }
        case "topic" => if( topic_prefix!=null ) {
          rc.append(topic_prefix)
        }
        case "dsub" => if( dsub_prefix!=null ) {
          rc.append(dsub_prefix)
        }
        case _ => throw sys.error("Uknown domain: "+address.domain);
      }
      rc.append(encode_path(address.path))
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
  def decode_multi_destination(value: String, unqualified:(String)=>SimpleAddress=null): Array[SimpleAddress] = {
    if (value == null) {
      return null;
    }

    if (destination_separator!=null && value.contains(destination_separator)) {
      var rc = value.split(Pattern.quote(destination_separator));
      var dl = ListBuffer[SimpleAddress]()
      for (buffer <- rc) {
        val d = decode_single_destination(buffer, unqualified)
        if (d == null) {
          return null;
        }
        dl += d
      }
      return dl.toArray
    } else {
      val rc = decode_single_destination(value, unqualified)
      if( rc == null ) {
        null
      } else {
        Array(rc)
      }
    }
  }

  /**
   * Parses a non-composite destination name.
   *
   * @param value
   * @param compositeSeparator
   * @return
   */
  def decode_single_destination(value: String, unqualified: (String) => SimpleAddress): SimpleAddress = {
    if (queue_prefix != null && value.startsWith(queue_prefix)) {
      var name = value.substring(queue_prefix.length)
      return new SimpleAddress("queue", decode_path(name))
    } else if (topic_prefix != null && value.startsWith(topic_prefix)) {
      var name = value.substring(topic_prefix.length)
      return new SimpleAddress("topic", decode_path(name))
    } else if (dsub_prefix != null && value.startsWith(dsub_prefix)) {
      var name = value.substring(dsub_prefix.length)
      return new SimpleAddress("dsub", decode_path(name))
    } else if (temp_topic_prefix != null && value.startsWith(temp_topic_prefix)) {
      var name = value.substring(temp_topic_prefix.length)
      return new SimpleAddress("temp-topic", decode_path(name))
    } else if (temp_queue_prefix != null && value.startsWith(temp_queue_prefix)) {
      var name = value.substring(temp_queue_prefix.length)
      return new SimpleAddress("temp-queue", decode_path(name))
    } else if (unqualified != null) {
      return unqualified(value)
    } else {
      return null
    }
  }

}

