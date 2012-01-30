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
import org.apache.activemq.apollo.broker.DestinationParser
import org.apache.activemq.apollo.openwire.command._
import java.lang.String

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
  OPENWIRE_PARSER.dsub_prefix = null
  OPENWIRE_PARSER.path_separator = "."
  OPENWIRE_PARSER.any_child_wildcard = "*"
  OPENWIRE_PARSER.any_descendant_wildcard = ">"
  OPENWIRE_PARSER.sanitize_destinations = true

  def to_destination_dto(dest: ActiveMQDestination, handler:OpenwireProtocolHandler): Array[DestinationDTO] = {
    def fallback(value:String) = {
      OPENWIRE_PARSER.decode_single_destination(dest.getQualifiedPrefix+value, null)
    }
    val rc = OPENWIRE_PARSER.decode_multi_destination(dest.getPhysicalName.toString, fallback)
    rc.foreach { dest =>
      if( dest.temp() ) {
        import collection.JavaConversions._
        // Put it back together...
        val name = dest.path.map(OPENWIRE_PARSER.unsanitize_destination_part(_)).mkString(OPENWIRE_PARSER.path_separator)

        val (connectionid, rest) = name.splitAt(name.lastIndexOf(':'))
        val real_path = ("temp" :: handler.broker.id :: OPENWIRE_PARSER.sanitize_destination_part(connectionid) :: OPENWIRE_PARSER.sanitize_destination_part(rest.substring(1)) :: Nil).toArray
        dest.path = java.util.Arrays.asList(real_path:_*)
      }
    }
    rc
  }

  def to_activemq_destination(dests:Array[DestinationDTO]):ActiveMQDestination = {
    import collection.JavaConversions._
    var wrapper: (String)=> ActiveMQDestination = null
    
    val rc = OPENWIRE_PARSER.encode_destination(dests.flatMap{ dest=>
      val temp = dest.path.headOption == Some("temp")
      dest match {
        case dest:QueueDestinationDTO =>
          if( temp ) {
            if(wrapper==null) 
              wrapper = (x)=>new ActiveMQTempQueue(x)
            var path: Array[String] = Array(dest.path.toList.drop(2).map(OPENWIRE_PARSER.unsanitize_destination_part(_)).mkString(":"))
            Some(new QueueDestinationDTO(path).temp(true))
          } else {
            if(wrapper==null) 
              wrapper = (x)=>new ActiveMQQueue(x)
            Some(dest)
          }
        case dest:TopicDestinationDTO =>
          if( temp ) {
            if(wrapper==null) 
              wrapper = (x)=>new ActiveMQTempTopic(x)
            var path: Array[String] = Array(dest.path.toList.drop(2).map(OPENWIRE_PARSER.unsanitize_destination_part(_)).mkString(":"))
            Some(new TopicDestinationDTO(path).temp(true))
          } else {
            if(wrapper==null) 
              wrapper = (x)=>new ActiveMQTopic(x)
            Some(dest)
          }
        case _ => None 
      }
    })

    if ( wrapper==null )
      null
    else
      wrapper(rc)
  }
}