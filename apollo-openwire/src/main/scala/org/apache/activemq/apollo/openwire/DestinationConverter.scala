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

import command._
import org.apache.activemq.apollo.util.path.Path
import java.util.regex.{Matcher, Pattern}
import org.apache.activemq.apollo.dto.{TopicDestinationDTO, QueueDestinationDTO, DestinationDTO}
import org.apache.activemq.apollo.broker.{DestinationParser, LocalRouter}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object DestinationConverter {

  val OPENWIRE_PARSER = new DestinationParser();
  OPENWIRE_PARSER.path_separator = "."
  OPENWIRE_PARSER.any_child_wildcard = "*"
  OPENWIRE_PARSER.any_descendant_wildcard = ">"

  def to_destination_dto(domain: String, parts:Array[String]): DestinationDTO = domain match {
    case LocalRouter.QUEUE_DOMAIN => new QueueDestinationDTO(parts)
    case LocalRouter.TOPIC_DOMAIN => new TopicDestinationDTO(parts)
    case _ => throw new Exception("Uknown destination domain: " + domain);
  }

  def to_destination_dto(domain: String, path: Path): Array[DestinationDTO] = {
      Array(to_destination_dto(domain, OPENWIRE_PARSER.path_parts(path)))
  }

  def to_destination_dto(dest: ActiveMQDestination): Array[DestinationDTO] = {
    if( !dest.isComposite ) {
      import ActiveMQDestination._
      val physicalName = dest.getPhysicalName.replaceAll(Pattern.quote(":"), Matcher.quoteReplacement("%58"))

      var path = OPENWIRE_PARSER.decode_path(physicalName)
      dest.getDestinationType match {
        case QUEUE_TYPE =>
          to_destination_dto(LocalRouter.QUEUE_DOMAIN, path)
        case TOPIC_TYPE =>
          to_destination_dto(LocalRouter.TOPIC_DOMAIN, path)
        case TEMP_QUEUE_TYPE =>
          to_destination_dto(LocalRouter.QUEUE_DOMAIN, Path("ActiveMQ", "Temp") + path)
        case TEMP_TOPIC_TYPE =>
          to_destination_dto(LocalRouter.TOPIC_DOMAIN, Path("ActiveMQ", "Temp") + path)
      }
    } else {
      dest.getCompositeDestinations.map { c =>
        to_destination_dto(c)(0)
      }
    }
  }

  def to_activemq_destination(dest:Array[DestinationDTO]):ActiveMQDestination = {
    import collection.JavaConversions._

    val rc = dest.map { dest =>
      var temp = dest.temp_owner != null
      val name = OPENWIRE_PARSER.encode_path(asScalaBuffer(dest.path).toList match {
        case "ActiveMQ" :: "Temp" :: rest =>
          temp = true
          rest
        case rest =>
          rest
      }).replaceAll(Pattern.quote("%58"), Matcher.quoteReplacement(":"))

      dest match {
        case dest:QueueDestinationDTO =>
          if( temp ) {
            new ActiveMQTempQueue(name)
          } else {
            new ActiveMQQueue(name)
          }
        case dest:TopicDestinationDTO =>
          if( temp ) {
            new ActiveMQTempTopic(name)
          } else {
            new ActiveMQTopic(name)
          }
      }
    }

    if( rc.length == 1) {
      rc(0)
    } else {
      val c = new ActiveMQQueue()
      c.setCompositeDestinations(rc)
      c
    }

  }
}