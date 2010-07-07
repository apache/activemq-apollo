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
package org.apache.activemq.broker.store.hawtdb

import model.RootRecord
import org.fusesource.hawtbuf.{Buffer, AsciiBuffer}
import org.fusesource.hawtdb.api._
import org.fusesource.hawtbuf.proto.{MessageBuffer, PBMessageFactory, PBMessage}
import java.io.{InputStream, OutputStream, DataOutputStream, DataInputStream}
import org.fusesource.hawtdb.internal.journal.LocationCodec
import org.fusesource.hawtbuf.codec._


object DestinationEntity {
  val MARSHALLER = LocationCodec.INSTANCE
}
class DestinationEntity {
}

//case class PBEncoderDecoder[Bean <: PBMessage[_,_] , Buffer <: MessageBuffer[_,_] ]( factory:PBMessageFactory[Bean, Buffer] ) extends AbstractStreamEncoderDecoder[Bean] {
//  protected def encode(paged: Paged, os: DataOutputStream, data: Bean) = data.freeze.asInstanceOf[Buffer].writeFramed( os.asInstanceOf[OutputStream] )
//  protected def decode(paged: Paged, is: DataInputStream) = factory.parseFramed( is.asInstanceOf[InputStream] ).copy.asInstanceOf[Bean]
//}

object RootEntity {
//  val messageKeyIndexFactory = new BTreeIndexFactory[Long, Long]();
//  val locationIndexFactory = new BTreeIndexFactory[Integer, Long]();
//  val messageRefsIndexFactory = new BTreeIndexFactory[Long, Long]();
//  val destinationIndexFactory = new BTreeIndexFactory[Long, DestinationEntity]();
//  val subscriptionIndexFactory = new BTreeIndexFactory[AsciiBuffer, Buffer]();
//  val mapIndexFactory = new BTreeIndexFactory[AsciiBuffer, Integer]();
//  val mapInstanceIndexFactory = new BTreeIndexFactory[AsciiBuffer, Buffer]();
//
//  messageKeyIndexFactory.setKeyCodec(LongCodec.INSTANCE);
//  messageKeyIndexFactory.setValueCodec(LongCodec.INSTANCE);
//  messageKeyIndexFactory.setDeferredEncoding(true);
//
//  locationIndexFactory.setKeyCodec(IntegerCodec.INSTANCE);
//  locationIndexFactory.setValueCodec(LongCodec.INSTANCE);
//  locationIndexFactory.setDeferredEncoding(true);
//
//  messageRefsIndexFactory.setKeyCodec(LongCodec.INSTANCE);
//  messageRefsIndexFactory.setValueCodec(LongCodec.INSTANCE);
//  messageRefsIndexFactory.setDeferredEncoding(true);
//
//  destinationIndexFactory.setKeyCodec(LongCodec.INSTANCE);
//  destinationIndexFactory.setValueCodec(DestinationEntity.MARSHALLER);
//  destinationIndexFactory.setDeferredEncoding(true);
//
//  subscriptionIndexFactory.setKeyCodec(Codecs.ASCII_BUFFER_CODEC);
//  subscriptionIndexFactory.setValueCodec(Codecs.BUFFER_CODEC);
//  subscriptionIndexFactory.setDeferredEncoding(true);
//
//  mapIndexFactory.setKeyCodec(Codecs.ASCII_BUFFER_CODEC);
//  mapIndexFactory.setValueCodec(IntegerCodec.INSTANCE);
//  mapIndexFactory.setDeferredEncoding(true);
//
//  val DATA_ENCODER_DECODER = PBEncoderDecoder(RootRecord.FACTORY)

}

/**
 *
 * @author[a href="http://hiramchirino.com"]Hiram Chirino[/a]
 */
class RootEntity {
//  var data: RootRecord.Bean
//
//  def allocate(tx: Transaction) = {
//
//    val rootPage = tx.alloc();
//    assert(rootPage == 0)
//
//    data = new RootRecord.Bean();
//    data.setMessageKeyIndexPage(tx.alloc)
//    data.setLocationIndexPage(tx.alloc)
//    data.setDestinationIndexPage(tx.alloc)
//    data.setMessageRefsIndexPage(tx.alloc)
//    data.setSubscriptionIndexPage(tx.alloc)
//    data.setMapIndexPage(tx.alloc)
//
//    tx.put(DATA_ENCODER_DECODER, rootPage, data);
//  }

//  def load(Transaction tx) throws IOException {
//    data = tx.get(DATA_ENCODER_DECODER, 0);
//
//    // Update max message key:
//    maxMessageKey = data.maxMessageKey;
//    Entry < Long, Location > last = data.messageKeyIndex.getLast();
//  if (last != null) {
//    if (last.getKey() > maxMessageKey) {
//      maxMessageKey = last.getKey();
//    }
//  }
//
//}
//
//def store (Transaction tx) throws IOException {
//// TODO: need ot make Data immutable..
//tx.put (DATA_ENCODER_DECODER, 0, data);
//}
}