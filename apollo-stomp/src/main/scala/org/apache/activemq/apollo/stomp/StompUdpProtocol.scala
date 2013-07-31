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
package org.apache.activemq.apollo.stomp

import org.apache.activemq.apollo.broker.protocol._
import org.apache.activemq.apollo.broker._
import org.fusesource.hawtbuf.Buffer
import org.apache.activemq.apollo.stomp.Stomp._
import org.apache.activemq.apollo.stomp.dto.StompDTO
import org.fusesource.hawtbuf.Buffer._
import scala.Some
import org.apache.activemq.apollo.broker.protocol.UdpMessage
import org.apache.activemq.apollo.broker.security.SecurityContext

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class StompUdpProtocol extends UdpProtocol {

  override def id = "stomp-udp"

  override def createProtocolHandler(connector:Connector) = new UdpProtocolHandler {

    type ConfigTypeDTO = StompDTO
    def configClass = classOf[ConfigTypeDTO]

    var config:ConfigTypeDTO = _
    var destination_parser = Stomp.destination_parser
    var protocol_filters = List[ProtocolFilter3]()
    var message_id_counter = 0L
    var default_virtual_host:VirtualHost = _

    override def configure(c: Option[ConfigTypeDTO]) = {
      config = c.getOrElse(new ConfigTypeDTO)
      import collection.JavaConversions._
      default_virtual_host = broker.default_virtual_host
      protocol_filters = ProtocolFilter3.create_filters(config.protocol_filters.toList, this)


//      Option(config.max_data_length).map(MemoryPropertyEditor.parse(_).toInt).foreach( codec.max_data_length = _ )
//      Option(config.max_header_length).map(MemoryPropertyEditor.parse(_).toInt).foreach( codec.max_header_length = _ )
//      config.max_headers.foreach( codec.max_headers = _ )

      if( config.queue_prefix!=null ||
          config.topic_prefix!=null ||
          config.destination_separator!=null ||
          config.path_separator!= null ||
          config.any_child_wildcard != null ||
          config.any_descendant_wildcard!= null ||
          config.regex_wildcard_start!= null ||
          config.regex_wildcard_end!= null
      ) {

        destination_parser = new DestinationParser().copy(Stomp.destination_parser)
        if( config.queue_prefix!=null ) { destination_parser.queue_prefix = config.queue_prefix }
        if( config.topic_prefix!=null ) { destination_parser.topic_prefix = config.topic_prefix }
        if( config.temp_queue_prefix!=null ) { destination_parser.temp_queue_prefix = config.temp_queue_prefix }
        if( config.temp_topic_prefix!=null ) { destination_parser.temp_topic_prefix = config.temp_topic_prefix }
        if( config.destination_separator!=null ) { destination_parser.destination_separator = config.destination_separator }
        if( config.path_separator!=null ) { destination_parser.path_separator = config.path_separator }
        if( config.any_child_wildcard!=null ) { destination_parser.any_child_wildcard = config.any_child_wildcard }
        if( config.any_descendant_wildcard!=null ) { destination_parser.any_descendant_wildcard = config.any_descendant_wildcard }
        if( config.regex_wildcard_start!=null ) { destination_parser.regex_wildcard_start = config.regex_wildcard_start }
        if( config.regex_wildcard_end!=null ) { destination_parser.regex_wildcard_end = config.regex_wildcard_end }

      }

    }

    def decode_address(dest:String):Array[SimpleAddress] = {
      val rc = destination_parser.decode_multi_destination(dest.toString)
      if( rc==null ) {
        throw new ProtocolException("Invalid stomp destination name: "+dest);
      }
      rc
    }

    def build_security_context(udp: UdpMessage, frame:StompFrame):(SecurityContext,StompFrame) = {
      import StompProtocolHandler._
      var headers = frame.headers
      val login = get(headers, LOGIN)
      val passcode = get(headers, PASSCODE)
      val sc = new SecurityContext
      sc.connector_id = connection.connector.id
      sc.local_address = connection.transport.getLocalAddress
      sc.remote_address = udp.from
      sc.session_id = session_id
      if( login.isDefined || passcode.isDefined ) {
        for( value <- login ) {
          sc.user = value.toString
          headers = headers.filterNot( _._1 == LOGIN)
        }
        for( value <- passcode ) {
          sc.password = value.toString
          headers = headers.filterNot( _._1 == PASSCODE)
        }
        (sc, frame.copy(headers=headers))
      } else {
        (sc, frame)
      }
    }

    def decode(udp: UdpMessage):Option[DecodedUdpMessage] = {
      import StompProtocolHandler._

      try {
        var frame = StompCodec.decode_frame(new Buffer(udp.buffer))

        if(!protocol_filters.isEmpty) {
          for( filter <- protocol_filters) {
            if( frame !=null ) {
              frame = filter.filter_inbound(frame)
            }
          }
          if( frame == null ) {
            return None
          }
        }

        val virtual_host = get(frame.headers, HOST) match {
          case Some(host) => broker.cow_virtual_hosts_by_hostname.get(host).getOrElse(default_virtual_host)
          case None => default_virtual_host
        }

        val (sc, updated_frame) = build_security_context(udp, frame)
        frame = updated_frame
        val dest = get(frame.headers, DESTINATION).get.deepCopy().ascii()

        Some(new DecodedUdpMessage {
          def message = udp
          def host = virtual_host
          def security_context = sc
          def address = dest

          def delivery = {

            // Apply header updates...
            val updated_frame = updated_headers(frame.headers, security_context) match {
              case Nil=> frame.copy(action=MESSAGE)
              case updated_headers => frame.copy(action=MESSAGE, updated_headers=updated_headers)
            }

            var message: StompFrameMessage = new StompFrameMessage(updated_frame)
            val delivery = new Delivery
            delivery.size = updated_frame.size
            delivery.message = message
            delivery.expiration = message.expiration
            delivery.persistent = message.persistent
            get(updated_frame.headers, RETAIN).foreach { retain =>
              delivery.retain = retain match {
                case SET => RetainSet
                case REMOVE => RetainRemove
                case _ => RetainIgnore
              }
            }
            delivery
          }
        })

      } catch {
        case e:Throwable => None
      }

    }

    def updated_headers(headers:HeaderMap, security_context:SecurityContext) = {
      import StompProtocolHandler._
      import collection.JavaConversions._

      var rc:HeaderMap=Nil
      val host = default_virtual_host

      // Do we need to add the message id?
      if( get( headers, MESSAGE_ID) == None ) {
        message_id_counter += 1
        rc ::= (MESSAGE_ID -> ascii(session_id+message_id_counter))
      }

      if( config.add_timestamp_header!=null ) {
        rc ::= (encode_header(config.add_timestamp_header), ascii(broker.now.toString()))
      }

      // Do we need to add the user id?
      if( host.authenticator!=null ) {
        if( config.add_user_header!=null ) {
          val value = host.authenticator.user_name(security_context).getOrElse("")
          rc ::= (encode_header(config.add_user_header), encode_header(value))
        }
        if( !config.add_user_headers.isEmpty ){
          config.add_user_headers.foreach { h =>
            val matches = security_context.principals(Option(h.kind).getOrElse("*"))
            val value = if( !matches.isEmpty ) {
              h.separator match {
                case null=>
                  matches.head.getName
                case separator =>
                  matches.map(_.getName).mkString(separator)
              }
            } else {
              ""
            }
            rc ::= (encode_header(h.name.trim), encode_header(value))
          }
        }
      }

      rc
    }

  }
}
