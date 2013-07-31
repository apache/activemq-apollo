/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.apollo.broker.protocol
import org.fusesource.hawtdispatch.transport.SslProtocolCodec
import org.fusesource.hawtbuf.Buffer
import org.apache.activemq.apollo.broker.Connector
import org.apache.activemq.apollo.dto.SslDTO
import org.fusesource.hawtdispatch.transport.SslProtocolCodec.ClientAuth

/**
 */
class SslProtocol extends Protocol {
  def id(): String = "ssl"

  override def isIdentifiable = true
  override def maxIdentificaionLength = 6

  override def matchesIdentification(buffer: Buffer):Boolean = {
    if( buffer.length >= 6 ) {
      if( buffer.get(0) == 0x16 ) { // content type
        (buffer.get(5) == 1) && // Client Hello
        ( (buffer.get(1) == 2) // SSLv2
          || (
            (buffer.get(1) == 3) && // SSLv3 or TLS
            (buffer.get(2) match {  // Minor version
              case 0 => true // SSLv3
              case 1 => true // TLSv1
              case 2 => true // TLSv2
              case 3 => true // TLSv3
              case _ => false
            })
          )
        )
      } else {
        // We have variable header offset..
        ((buffer.get(0) & 0xC0) == 0x80) && // The rest of byte 0 and 1 are holds the record length.
          (buffer.get(2) == 1) && // Client Hello
          ( (buffer.get(3) == 2) // SSLv2
            || (
              (buffer.get(3) == 3) && // SSLv3 or TLS
              (buffer.get(4) match {  // Minor version
                case 0 => true // SSLv3
                case 1 => true // TLSv1
                case 2 => true // TLSv2
                case 3 => true // TLSv3
                case _ => false
              })
            )
          )
      }
    } else {
      false
    }
  }

  def createProtocolCodec(connector:Connector) = {
    val config = connector.protocol_codec_config(classOf[SslDTO]).getOrElse(new SslDTO)
    val client_auth =  if( config.client_auth!=null ) {
      ClientAuth.valueOf(config.client_auth.toUpperCase());
    } else {
      ClientAuth.WANT
    }

    val version = if( config.version!=null ) {
      config.version;
    } else {
      "SSL"
    }

    val rc = new SslProtocolCodec()
    rc.setSSLContext(connector.broker.ssl_context(version))
    rc.server(client_auth);
    rc.setNext(new AnyProtocolCodec(connector))
    rc
  }

  def createProtocolHandler(connector:Connector) = new AnyProtocolHandler
}
