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
package org.apache.activemq.apollo.broker.store.leveldb

import java.{lang=>jl}
import java.{util=>ju}

import java.util.zip.CRC32
import java.util.Map.Entry
import collection.immutable.TreeMap
import org.fusesource.hawtdispatch.BaseRetained
import java.util.concurrent.atomic.AtomicLong
import java.io._
import org.apache.activemq.apollo.util.FileSupport._
import org.apache.activemq.apollo.util.{Log, LRUCache}
import org.fusesource.hawtbuf.{DataByteArrayInputStream, DataByteArrayOutputStream, Buffer}

object RecordLog extends Log {

  // The log files contain a sequence of variable length log records:
  // record := header + data
  //
  // header :=
  //   '*'      : int8       // Start of Record Magic
  //   kind     : int8       // Help identify content type of the data.
  //   checksum : uint32     // crc32c of the data[]
  //   length   : uint32     // the length the the data

  val LOG_HEADER_PREFIX = '*'.toByte
  val LOG_HEADER_SIZE = 10
  val BUFFER_SIZE = 1024
}

case class RecordLog(directory: File, logSuffix:String) {
  import RecordLog._

  directory.mkdirs()

  var logSize = 1024 * 1024 * 100
  var current_appender:LogAppender = _
  var paranoidChecks = false
  var sync = false

  case class LogInfo(file:File, position:Long, length:AtomicLong) {
    def limit = position+length.get
  }

  var log_infos = TreeMap[Long, LogInfo]()
  object log_mutex

  def delete(id:Long) = {
    log_mutex.synchronized {
      // We can't delete the current appender.
      if( current_appender.start != id ) {
        log_infos.get(id).foreach { info =>
          onDelete(info.file)
          log_infos = log_infos.filterNot(_._1 == id)
        }
      }
    }
  }

  protected def onDelete(file:File) = {
    file.delete()
  }

  def checksum(data: Buffer): Int = {
    val checksum = new CRC32
    checksum.update(data.data, data.offset, data.length)
    (checksum.getValue & 0xFFFFFFFF).toInt
  }

  class LogAppender(file:File, start:Long) extends LogReader(file, start) {

    override def open = new RandomAccessFile(file, "rw")

    override def dispose() = {
      force
      super.dispose()
    }

    val length = new AtomicLong(0)
    
    def limit = start+length.get()

    // set the file size ahead of time so that we don't have to sync the file
    // meta-data on every log sync.
    channel.position(logSize-1)
    channel.write(new Buffer(1).toByteBuffer)
    channel.force(true)
    channel.position(0)

    val os = new DataByteArrayOutputStream((BUFFER_SIZE)+LOG_HEADER_PREFIX)

    def force = {
      // only need to update the file metadata if the file size changes..
      flush
      if(sync) {
        channel.force(length.get() > logSize)
      }
    }

    def flush = {
      if( os.position() > 0 ) {
        val buffer = os.toBuffer.toByteBuffer
        val pos = length.get()-buffer.remaining
        trace("wrote at "+pos+" "+os.toBuffer)
        channel.write(buffer, pos)
        if( buffer.hasRemaining ) {
          throw new IOException("Short write")
        }
        os.reset()
      }
    }

    /**
     * returns the offset position of the data record.
     */
    def append(id:Byte, data: Buffer): Long = {
      val rc = limit
      val data_length = data.length
      val total_length = LOG_HEADER_SIZE + data_length
      
      if( os.position() + total_length > BUFFER_SIZE ) {
        flush
      }

      if( total_length > (BUFFER_SIZE<<2) ) {

        // Write the header and flush..
        os.writeByte(LOG_HEADER_PREFIX)
        os.writeByte(id)
        os.writeInt(checksum(data))
        os.writeInt(data_length)

        length.addAndGet(LOG_HEADER_PREFIX)
        flush

        // Directly write the data to the channel since it's large.
        val buffer = data.toByteBuffer
        val pos = length.get()+LOG_HEADER_PREFIX
        trace("wrote at "+pos+" "+data)
        channel.write(buffer, pos)
        if( buffer.hasRemaining ) {
          throw new IOException("Short write")
        }
        length.addAndGet(data_length)

      } else {
        os.writeByte(LOG_HEADER_PREFIX)
        os.writeByte(id)
        os.writeInt(checksum(data))
        os.writeInt(data_length)
        os.write(data.data, data.offset, data_length)
        length.addAndGet(total_length)
      }
      rc
    }

  }

  case class LogReader(file:File, start:Long) extends BaseRetained {
    
    val fd = open

    def open = new RandomAccessFile(file, "r")
    
    def channel = fd.getChannel

    override def dispose() {
      fd.close()
    }

    def read(pos:Long, length:Int) = this.synchronized {
      val offset = (pos-start).toInt
      if(paranoidChecks) {
        val record = new Buffer(LOG_HEADER_SIZE+length)
        if( channel.read(record.toByteBuffer, offset) != record.length ) {
          val data2 = new Buffer(LOG_HEADER_SIZE+length)
          channel.read(data2.toByteBuffer, offset)
          throw new IOException("short record at position: "+pos+" in file: "+file+", offset: "+offset)
        }

        val is = new DataByteArrayInputStream(record)
        val prefix = is.readByte()
        if( prefix != LOG_HEADER_PREFIX ) {
          throw new IOException("invalid record at position: "+pos+" in file: "+file+", offset: "+offset)
        }

        val id = is.readByte()
        val expectedChecksum = is.readInt()
        val expectedLength = is.readInt()
        val data = is.readBuffer(length)

        // If your reading the whole record we can verify the data checksum
        if( expectedLength == length ) {
          if( expectedChecksum != checksum(data) ) {
            throw new IOException("checksum does not match at position: "+pos+" in file: "+file+", offset: "+offset)
          }
        }

        data
      } else {
        val data = new Buffer(length)
        if( channel.read(data.toByteBuffer, offset+LOG_HEADER_SIZE) != data.length ) {
          throw new IOException("short record at position: "+pos+" in file: "+file+", offset: "+offset)
        }
        data
      }
    }

    def read(pos:Long) = this.synchronized {
      val offset = (pos-start).toInt
      val header = new Buffer(LOG_HEADER_SIZE)
      channel.read(header.toByteBuffer, offset)
      val is = header.bigEndianEditor();
      val prefix = is.readByte()
      if( prefix != LOG_HEADER_PREFIX ) {
        // Does not look like a record.
        throw new IOException("invalid record position")
      }
      val id = is.readByte()
      val expectedChecksum = is.readInt()
      val length = is.readInt()
      val data = new Buffer(length)

      if( channel.read(data.toByteBuffer, offset+LOG_HEADER_SIZE) != length ) {
        throw new IOException("short record")
      }

      if(paranoidChecks) {
        if( expectedChecksum != checksum(data) ) {
          throw new IOException("checksum does not match")
        }
      }
      (id, data, pos+LOG_HEADER_SIZE+length)
    }

    def check(pos:Long):Option[Long] = this.synchronized {
      var offset = (pos-start).toInt
      val header = new Buffer(LOG_HEADER_SIZE)
      channel.read(header.toByteBuffer, offset)
      val is = header.bigEndianEditor();
      val prefix = is.readByte()
      if( prefix != LOG_HEADER_PREFIX ) {
        return None // Does not look like a record.
      }
      val id = is.readByte()
      val expectedChecksum = is.readInt()
      val length = is.readInt()

      val chunk = new Buffer(1024*4)
      val chunkbb = chunk.toByteBuffer
      offset += LOG_HEADER_SIZE

      // Read the data in in chunks to avoid
      // OOME if we are checking an invalid record
      // with a bad record length
      val checksumer = new CRC32
      var remaining = length
      while( remaining > 0 ) {
        val chunkSize = remaining.min(1024*4);
        chunkbb.position(0)
        chunkbb.limit(chunkSize)
        channel.read(chunkbb, offset)
        if( chunkbb.hasRemaining ) {
          return None
        }
        checksumer.update(chunk.data, 0, chunkSize)
        offset += chunkSize
        remaining -= chunkSize
      }

      val checksum = ( checksumer.getValue & 0xFFFFFFFF).toInt
      if( expectedChecksum !=  checksum ) {
        return None
      }
      return Some(pos+LOG_HEADER_SIZE+length)
    }

    def verifyAndGetEndPosition:Long = this.synchronized {
      var pos = start;
      val limit = start+channel.size()
      while(pos < limit) {
        check(pos) match {
          case Some(next) => pos = next
          case None => return pos
        }
      }
      pos
    }
  }

  def create_log_appender(position: Long) = {
    new LogAppender(next_log(position), position)
  }

  def create_appender(position: Long): Any = {
    current_appender = create_log_appender(position)
    log_mutex.synchronized {
      log_infos += position -> new LogInfo(current_appender.file, position, current_appender.length)
    }
  }

  def open = {
    log_mutex.synchronized {
      log_infos = LevelDBClient.find_sequence_files(directory, logSuffix).map { case (position,file) =>
        position -> LogInfo(file, position, new AtomicLong(file.length()))
      }

      val appendPos = if( log_infos.isEmpty ) {
        0L
      } else {
        val (_, file) = log_infos.last
        val r = LogReader(file.file, file.position)
        try {
          val rc = r.verifyAndGetEndPosition
          file.length.set(rc - file.position)
          if( file.file.length != file.length.get() ) {
            // we need to truncate.
            using(new RandomAccessFile(file.file, "rw")) ( _.setLength(file.length.get()) )
          }
          rc
        } finally {
          r.release()
        }
      }

      create_appender(appendPos)
    }
  }

  def close = {
    log_mutex.synchronized {
      current_appender.release
    }
  }

  def appender_limit = current_appender.limit
  def appender_start = current_appender.start

  def next_log(position:Long) = LevelDBClient.create_sequence_file(directory, position, logSuffix)

  def appender[T](func: (LogAppender)=>T):T= {
    try {
      func(current_appender)
    } finally {
      current_appender.flush
      log_mutex.synchronized {
        if ( current_appender.length.get >= logSize ) {
          current_appender.release()
          on_log_rotate()
          create_appender(current_appender.limit)
        }
      }
    }
  }

  var on_log_rotate: ()=>Unit = ()=>{}

  private val reader_cache = new LRUCache[File, LogReader](100) {
    protected override def onCacheEviction(entry: Entry[File, LogReader]) = {
      entry.getValue.release()
    }
  }

  def log_info(pos:Long) = log_mutex.synchronized(log_infos.range(0L, pos+1).lastOption.map(_._2))

  private def get_reader[T](pos:Long)(func: (LogReader)=>T) = {

    val lookup = log_mutex.synchronized {
      val info = log_info(pos)
      info.map { info=>
        if(info.position == current_appender.start) {
          current_appender.retain()
          (info, current_appender)
        } else {
          (info, null)
        }
      }
    }

    lookup.map { case (info, appender) =>
      val reader = if( appender!=null ) {
        // read from the current appender.
        appender
      } else {
        // Checkout a reader from the cache...
        reader_cache.synchronized {
          var reader = reader_cache.get(info.file)
          if(reader==null) {
            reader = LogReader(info.file, info.position)
            reader_cache.put(info.file, reader)
          }
          reader.retain()
          reader
        }
      }

      try {
        func(reader)
      } finally {
        reader.release
      }
    }
  }

  def read(pos:Long) = {
    get_reader(pos)(_.read(pos))
  }
  def read(pos:Long, length:Int) = {
    get_reader(pos)(_.read(pos, length))
  }

}
