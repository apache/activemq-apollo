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
import org.apache.activemq.apollo.util.path.{Path, PathParser}
import Buffer._

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class DestinationParser extends PathParser {

  var default_domain: AsciiBuffer = null
  var queue_prefix: AsciiBuffer = ascii("queue:")
  var topic_prefix: AsciiBuffer = ascii("topic:")
  var temp_queue_prefix: AsciiBuffer = ascii("temp-queue:")
  var temp_topic_prefix: AsciiBuffer = ascii("temp-topic:")
  var destination_separator: Option[Byte] = Some(','.toByte)

  def toBuffer(value: Destination): AsciiBuffer = {
    if (value == null) {
      null
    } else {
      val baos = new ByteArrayOutputStream
      def write(value: Destination):Unit = {
        if (value.getDestinations != null) {
          assert( destination_separator.isDefined )
          val first = true
          for (d <- value.getDestinations) {
            if (!first) {
              baos.write(destination_separator.get)
            }
            write(d)
          }
        } else {
          value.getDomain match {
            case Router.QUEUE_DOMAIN =>
              baos.write(queue_prefix)
            case Router.TOPIC_DOMAIN =>
              baos.write(topic_prefix)
            case Router.TEMP_QUEUE_DOMAIN =>
              baos.write(temp_queue_prefix)
            case Router.TEMP_TOPIC_DOMAIN =>
              baos.write(temp_topic_prefix)
          }
          this.write(value.path, baos)
        }
      }
      write(value)
      baos.toBuffer.ascii
    }
  }

  /**
   * Parses a destination which may or may not be a composite.
   *
   * @param value
   * @param compositeSeparator
   * @return
   */
  def parse(value: AsciiBuffer): Destination = {
    if (value == null) {
      return null;
    }

    if (destination_separator.isDefined && value.contains(destination_separator.get)) {
      var rc = value.split(destination_separator.get);
      var dl: List[Destination] = Nil
      for (buffer <- rc) {
        val d = parse(buffer)
        if (d == null) {
          return null;
        }
        dl = dl ::: d :: Nil
      }
      return new MultiDestination(dl.toArray[Destination]);
    } else {
      if (queue_prefix != null && value.startsWith(queue_prefix)) {
        var name = value.slice(queue_prefix.length, value.length).ascii();
        return new SingleDestination(Router.QUEUE_DOMAIN, parsePath(name));
      } else if (topic_prefix != null && value.startsWith(topic_prefix)) {
        var name = value.slice(topic_prefix.length, value.length).ascii();
        return new SingleDestination(Router.TOPIC_DOMAIN, parsePath(name));
      } else if (temp_queue_prefix != null && value.startsWith(temp_queue_prefix)) {
        var name = value.slice(temp_queue_prefix.length, value.length).ascii();
        return new SingleDestination(Router.TEMP_QUEUE_DOMAIN, parsePath(name));
      } else if (temp_topic_prefix != null && value.startsWith(temp_topic_prefix)) {
        var name = value.slice(temp_topic_prefix.length, value.length).ascii();
        return new SingleDestination(Router.TEMP_TOPIC_DOMAIN, parsePath(name));
      } else {
        if (default_domain == null) {
          return null;
        }
        return new SingleDestination(default_domain, parsePath(value));
      }
    }
  }
}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
case class SingleDestination(var domain: AsciiBuffer = null, var name: Array[Path] = null) extends Destination {
  def getDestinations(): Array[Destination] = null;
  def getDomain(): AsciiBuffer = domain

  def path() = name

  override def toString() = "" + domain + ":" + name
}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
case class MultiDestination(var destinations: Array[Destination]) extends Destination {
  def getDestinations(): Array[Destination] = destinations;
  def getDomain(): AsciiBuffer = null

  def path() = null

  override def toString() = destinations.mkString(",")
}
