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

import org.apache.activemq.apollo.broker.Connector
import org.apache.activemq.apollo.util.ClassFinder
import org.fusesource.hawtbuf.Buffer
import org.fusesource.hawtdispatch.transport.ProtocolCodec

object ProtocolCodecFactory {
  abstract trait Provider {
    def id: String

    /**
     * @return an instance of the wire format.
     *
     */
    def createProtocolCodec(connector: Connector): ProtocolCodec

    /**
     * @return true if this wire format factory is identifiable. An identifiable
     *         protocol will first write a easy to identify header to the stream
     */
    def isIdentifiable: Boolean

    /**
     * @return Returns the maximum length of the header used to discriminate the wire format if it
     *         { @link #isIdentifiable()}
     * @throws UnsupportedOperationException If { @link #isIdentifiable()} is false
     */
    def maxIdentificaionLength: Int

    /**
     * Called to test if this protocol matches the identification header.
     *
     * @param buffer The byte buffer representing the header data read so far.
     * @return true if the Buffer matches the protocol format header.
     */
    def matchesIdentification(buffer: Buffer): Boolean
  }

  final val providers: ClassFinder[ProtocolCodecFactory.Provider] = new ClassFinder[ProtocolCodecFactory.Provider]("META-INF/services/org.apache.activemq.apollo/protocol-codec-factory.index", classOf[ProtocolCodecFactory.Provider])

  /**
   * Gets the provider.
   */
  def get(name: String): ProtocolCodecFactory.Provider = {
    import scala.collection.JavaConversions._
    for (provider <- providers.jsingletons) {
      if (name == provider.id) {
        return provider
      }
    }
    return null
  }

}