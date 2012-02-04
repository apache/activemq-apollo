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
package org.apache.activemq.apollo.openwire

import org.apache.activemq.apollo.dto.{TopicDestinationDTO, QueueDestinationDTO, DestinationDTO}
import org.apache.activemq.apollo.openwire.command._
import java.lang.String
import org.apache.activemq.apollo.broker.{DestinationAddress, SimpleAddress, DestinationParser}
import org.apache.activemq.apollo.util.path.{Path, LiteralPart}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object DestinationConverter {

  val OPENWIRE_PARSER = new DestinationParser();
  OPENWIRE_PARSER.queue_prefix = ActiveMQDestination.QUEUE_QUALIFIED_PREFIX
  OPENWIRE_PARSER.topic_prefix = ActiveMQDestination.TOPIC_QUALIFIED_PREFIX
  OPENWIRE_PARSER.temp_queue_prefix = ActiveMQDestination.TEMP_QUEUE_QUALIFED_PREFIX
  OPENWIRE_PARSER.temp_topic_prefix = ActiveMQDestination.TEMP_TOPIC_QUALIFED_PREFIX
  OPENWIRE_PARSER.path_separator = "."
  OPENWIRE_PARSER.any_child_wildcard = "*"
  OPENWIRE_PARSER.any_descendant_wildcard = ">"
  OPENWIRE_PARSER.part_pattern = null
  OPENWIRE_PARSER.regex_wildcard_start = null
  OPENWIRE_PARSER.regex_wildcard_end = null

  def to_destination_dto(dest: ActiveMQDestination, handler:OpenwireProtocolHandler): Array[SimpleAddress] = {
    def fallback(value:String) = {
      OPENWIRE_PARSER.decode_single_destination(dest.getQualifiedPrefix+value, null)
    }
    OPENWIRE_PARSER.decode_multi_destination(dest.getPhysicalName.toString, fallback).map { dest =>
      if( dest.domain.startsWith("temp-") ) {
        // Put it back together...
        val name = OPENWIRE_PARSER.encode_path(dest.path)
        val (connectionid, rest) = name.splitAt(name.lastIndexOf(':'))
        val real_path = "temp" :: handler.broker.id :: connectionid :: rest.substring(1) :: Nil
        SimpleAddress(dest.domain.stripPrefix("temp-"), OPENWIRE_PARSER.decode_path(real_path)) 
      } else {
        dest
      }
    }
  }

  def to_activemq_destination(addresses:Array[_ <: DestinationAddress]):ActiveMQDestination = {
    ActiveMQDestination.createDestination(OPENWIRE_PARSER.encode_destination(addresses.map{ address=>
      address.path.parts match {
        // Remap temp destinations that look like openwire temp destinations.
        case LiteralPart("temp") :: LiteralPart(broker) :: LiteralPart(session) :: LiteralPart(tempid) :: Nil =>
          if( session.startsWith("ID:") ) {
            SimpleAddress("temp-"+address.domain, Path(session+":"+tempid))
          } else {
            address
          }
        case _ => address
      }
    }))

  }
}