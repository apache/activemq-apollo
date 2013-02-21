package org.apache.activemq.apollo.broker.store

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
import org.apache.activemq.apollo.dto.StoreStatusDTO
import org.apache.activemq.apollo.util._
import java.util.concurrent.atomic.AtomicReference
import org.apache.activemq.apollo.util.tar._
import java.util.zip.{GZIPInputStream, GZIPOutputStream}
import org.fusesource.hawtbuf.{AsciiBuffer, Buffer}
import java.io._
import org.apache.activemq.apollo.util.FileSupport._
import org.fusesource.hawtbuf.proto.MessageBuffer

case class ExportStreamManager(target:OutputStream, version:Int) {
  val stream = new TarOutputStream(new GZIPOutputStream(target))

  var seq:Long = 0;
  
  def finish = stream.close()

  private def store(ext:String, value:Buffer) = {
    var entry = new TarEntry(seq.toString + "." + ext)
    seq += 1
    entry.setSize(value.length())
    stream.putNextEntry(entry);
    value.writeTo(stream)
    stream.closeEntry();
  }

  private def store(ext:String, value:MessageBuffer[_,_]) = {
    var entry = new TarEntry(seq.toString + "." + ext)
    seq += 1
    entry.setSize(value.serializedSizeFramed())
    stream.putNextEntry(entry);
    value.writeFramed(stream)
    stream.closeEntry();
  }

  store("ver", new AsciiBuffer(version.toString))

  def store_queue(value:QueuePB.Getter) = {
    store("que", value.freeze())
  }
  def store_queue_entry(value:QueueEntryPB.Getter) = {
    store("qen", value.freeze())
  }
  def store_message(value:MessagePB.Getter) = {
    store("msg", value.freeze())
  }
  def store_map_entry(value:MapEntryPB.Getter) = {
    store("map", value.freeze())
  }

}

case class ImportStreamManager(source:InputStream) {
  
  val stream = new TarInputStream(new GZIPInputStream(source))

  val version = try {
    var entry = stream.getNextEntry
    if( entry.getName != "0.ver" ) {
      throw new Exception("0.ver entry missing")
    }
    read_text(stream).toInt
  } catch {
    case e:Throwable => new IOException("Could not determine export format version: "+e)
  }
  
  def getNext:AnyRef = {
    var entry = stream.getNextEntry
    if( entry==null ) {
      return null;
    }

    if( entry.getName.endsWith(".qen") ) {
      QueueEntryPB.FACTORY.parseFramed(stream)
    } else if( entry.getName.endsWith(".msg") ) {
      MessagePB.FACTORY.parseFramed(stream)
    } else if( entry.getName.endsWith(".que") ) {
      QueuePB.FACTORY.parseFramed(stream)
    } else if( entry.getName.endsWith(".map") ) {
      MapEntryPB.FACTORY.parseFramed(stream)
    } else {
      throw new Exception("Unknown entry: "+entry.getName)
    }
  }
}


/**
 * <p>
 * The Store is service which offers asynchronous persistence services
 * to a Broker.
 * </p>
 *
 *  @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait Store extends ServiceTrait {

  def kind:String

  def location:String

  def get_store_status(callback:(StoreStatusDTO)=>Unit)

  /**
   * Creates a store uow which is used to perform persistent
   * operations as unit of work.
   */
  def create_uow:StoreUOW

  /**
   * Removes all previously stored data.
   */
  def purge(callback: =>Unit):Unit

  /**
   * Ges the last queue key identifier stored.
   */
  def get_last_queue_key(callback:(Option[Long])=>Unit):Unit

  /**
   * Adds a queue.
   *
   * This method auto generates and assigns the key field of the queue record and
   * returns true if it succeeded.
   */
  def add_queue(record:QueueRecord)(callback:(Boolean)=>Unit):Unit

  /**
   * Removes a queue. Success is reported via the callback.
   */
  def remove_queue(queueKey:Long)(callback:(Boolean)=>Unit):Unit

  /**
   * Gets a value of a previously stored map entry.
   */
  def get_map_entry(key:Buffer)(callback:(Option[Buffer])=>Unit )

  /**
   * Gets a value of a previously stored map entry.
   */
  def get_prefixed_map_entries(prefix:Buffer)(callback: Seq[(Buffer, Buffer)]=>Unit)

  /**
   * Loads the queue information for a given queue key.
   */
  def get_queue(queueKey:Long)(callback:(Option[QueueRecord])=>Unit )

  /**
   * Gets a listing of all queue entry sequences previously added
   * and reports them to the callback.
   */
  def list_queues(callback: (Seq[Long])=>Unit )

  /**
   * Groups all the entries in the specified queue into ranges containing up limit entries
   * big and returns those ranges.  Allows you to incrementally, load all the entries in
   * a queue.
   */
  def list_queue_entry_ranges(queueKey:Long, limit:Int)(callback:(Seq[QueueEntryRange])=>Unit )

  /**
   * Loads all the queue entry records for the given queue id between the first and last provided
   * queue sequences (inclusive).
   */
  def list_queue_entries(queueKey:Long, firstSeq:Long, lastSeq:Long)(callback:(Seq[QueueEntryRecord])=>Unit )

  /**
   * Removes a the delivery associated with the provided from any
   * internal buffers/caches.  The callback is executed once, the message is
   * no longer buffered.
   */
  def flush_message(messageKey:Long)(callback: =>Unit)

  /**
   * Loads a delivery with the associated id from persistent storage.
   */
  def load_message(messageKey:Long, locator:AtomicReference[Object])(callback:(Option[MessageRecord])=>Unit )

  /**
   * Exports the contents of the store to the provided stream.
   */
  def export_data(os:OutputStream, cb:(Option[String])=>Unit):Unit

  /**
   * Imports a previous export from the input stream.
   */
  def import_data(is:InputStream, cb:(Option[String])=>Unit):Unit

  /**
   * Compacts the data in the store.
   */
  def compact(callback: =>Unit) = callback

}