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

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class ParserOptions {
  var defaultDomain: AsciiBuffer = null
  var queuePrefix: AsciiBuffer = null
  var topicPrefix: AsciiBuffer = null
  var tempQueuePrefix: AsciiBuffer = null
  var tempTopicPrefix: AsciiBuffer = null
  var separator: Option[Byte] = None
}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object DestinationParser {

  def toBuffer(value: Destination, options: ParserOptions): AsciiBuffer = {
    if (value == null) {
      null
    } else {
      val baos = new ByteArrayOutputStream
      def write(value: Destination):Unit = {
        if (value.getDestinations != null) {
          assert( options.separator.isDefined )
          val first = true
          for (d <- value.getDestinations) {
            if (!first) {
              baos.write(options.separator.get)
            }
            write(d)
          }
        } else {
          value.getDomain match {
            case Router.QUEUE_DOMAIN =>
              baos.write(options.queuePrefix)
            case Router.TOPIC_DOMAIN =>
              baos.write(options.topicPrefix)
            case Router.TEMP_QUEUE_DOMAIN =>
              baos.write(options.tempQueuePrefix)
            case Router.TEMP_TOPIC_DOMAIN =>
              baos.write(options.tempTopicPrefix)
          }
          baos.write(value.getName)
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
   * @param options
   * @param compositeSeparator
   * @return
   */
  def parse(value: AsciiBuffer, options: ParserOptions): Destination = {
    if (value == null) {
      return null;
    }

    if (options.separator.isDefined && value.contains(options.separator.get)) {
      var rc = value.split(options.separator.get);
      var dl: List[Destination] = Nil
      for (buffer <- rc) {
        val d = parse(buffer, options)
        if (d == null) {
          return null;
        }
        dl = dl ::: d :: Nil
      }
      return new MultiDestination(dl.toArray[Destination]);
    } else {
      if (options.queuePrefix != null && value.startsWith(options.queuePrefix)) {
        var name = value.slice(options.queuePrefix.length, value.length).ascii();
        return new SingleDestination(Router.QUEUE_DOMAIN, name);
      } else if (options.topicPrefix != null && value.startsWith(options.topicPrefix)) {
        var name = value.slice(options.topicPrefix.length, value.length).ascii();
        return new SingleDestination(Router.TOPIC_DOMAIN, name);
      } else if (options.tempQueuePrefix != null && value.startsWith(options.tempQueuePrefix)) {
        var name = value.slice(options.tempQueuePrefix.length, value.length).ascii();
        return new SingleDestination(Router.TEMP_QUEUE_DOMAIN, name);
      } else if (options.tempTopicPrefix != null && value.startsWith(options.tempTopicPrefix)) {
        var name = value.slice(options.tempTopicPrefix.length, value.length).ascii();
        return new SingleDestination(Router.TEMP_TOPIC_DOMAIN, name);
      } else {
        if (options.defaultDomain == null) {
          return null;
        }
        return new SingleDestination(options.defaultDomain, value);
      }
    }
  }
}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
case class SingleDestination(var domain: AsciiBuffer = null, var name: AsciiBuffer = null) extends Destination {
  def getDestinations(): Array[Destination] = null;
  def getDomain(): AsciiBuffer = domain

  def getName(): AsciiBuffer = name

  override def toString() = "" + domain + ":" + name
}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
case class MultiDestination(var destinations: Array[Destination]) extends Destination {
  def getDestinations(): Array[Destination] = destinations;
  def getDomain(): AsciiBuffer = null

  def getName(): AsciiBuffer = null

  override def toString() = destinations.mkString(",")
}
