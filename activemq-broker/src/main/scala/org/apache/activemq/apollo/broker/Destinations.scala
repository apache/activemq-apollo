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

import _root_.org.apache.activemq.util.buffer.{AsciiBuffer}
import BufferConversions._

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class ParserOptions {
  var defaultDomain:AsciiBuffer = null
  var queuePrefix:AsciiBuffer = null
  var topicPrefix:AsciiBuffer = null
  var tempQueuePrefix:AsciiBuffer = null
  var tempTopicPrefix:AsciiBuffer = null
}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object DestinationParser {

    /**
     * Parses a simple destination.
     *
     * @param value
     * @param options
     * @return
     */
    def parse(value:AsciiBuffer, options:ParserOptions ):Destination = {
        if (options.queuePrefix!=null && value.startsWith(options.queuePrefix)) {
            var name = value.slice(options.queuePrefix.length, value.length).ascii();
            return new SingleDestination(Domain.QUEUE_DOMAIN, name);
        } else if (options.topicPrefix!=null && value.startsWith(options.topicPrefix)) {
            var name = value.slice(options.topicPrefix.length, value.length).ascii();
            return new SingleDestination(Domain.TOPIC_DOMAIN, name);
        } else if (options.tempQueuePrefix!=null && value.startsWith(options.tempQueuePrefix)) {
            var name = value.slice(options.tempQueuePrefix.length, value.length).ascii();
            return new SingleDestination(Domain.TEMP_QUEUE_DOMAIN, name);
        } else if (options.tempTopicPrefix!=null && value.startsWith(options.tempTopicPrefix)) {
            var name = value.slice(options.tempTopicPrefix.length, value.length).ascii();
            return new SingleDestination(Domain.TEMP_TOPIC_DOMAIN, name);
        } else {
            if( options.defaultDomain==null ) {
                return null;
            }
            return new SingleDestination(options.defaultDomain, value);
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
    def parse(value:AsciiBuffer, options:ParserOptions , compositeSeparator:Byte ):Destination = {
        if( value == null ) {
            return null;
        }

        if( value.contains(compositeSeparator) ) {
            var rc = value.split(compositeSeparator);
            var dl:List[Destination] = Nil
            for (buffer <- rc) {
              val d = parse(buffer, options)
              if( d==null ) {
                return null;
              }
              dl = dl ::: d :: Nil
            }
            return new MultiDestination(dl.toArray[Destination]);
        }
        return parse(value, options);
    }
}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
case class SingleDestination(var domain:AsciiBuffer=null, var name:AsciiBuffer=null) extends Destination {

  def getDestinations():Array[Destination] = null;
  def getDomain():AsciiBuffer = domain
  def getName():AsciiBuffer = name

  override def toString() = ""+domain+":"+name
}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
case class MultiDestination(var destinations:Array[Destination]) extends Destination {

  def getDestinations():Array[Destination] = destinations;
  def getDomain():AsciiBuffer = null
  def getName():AsciiBuffer = null

  override def toString() = destinations.mkString(",")
}
