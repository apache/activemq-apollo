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

import org.apache.activemq.apollo.broker.jaxb.PropertiesReader
import org.apache.activemq.apollo.dto.{XmlCodec, ConnectorDTO, VirtualHostDTO, BrokerDTO}
import java.util.regex.Pattern
import javax.xml.stream.{XMLOutputFactory, XMLInputFactory}
import org.fusesource.hawtdispatch._
import _root_.org.fusesource.hawtdispatch.ScalaDispatchHelpers._
import java.util.concurrent.{TimeUnit, ExecutorService, Executors}
import org.fusesource.hawtbuf.{ByteArrayInputStream, ByteArrayOutputStream}
import javax.xml.bind.{Marshaller, JAXBContext}
import security.EncryptionSupport
import XmlCodec._
import org.apache.activemq.apollo.util._
import scala.util.continuations._
import org.fusesource.hawtdispatch.DispatchQueue
import java.util.Arrays
import FileSupport._
import java.util.Properties
import java.io.{FileInputStream, OutputStreamWriter, File}

object ConfigStore {

  var store:ConfigStore = null

  def apply():ConfigStore = store

  def update(value:ConfigStore) = store=value

}

/**
 * <p>
 * Defines an interface to access and update persistent broker configurations.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait ConfigStore {

  def load(eval:Boolean): BrokerDTO

  def store(config:BrokerDTO): Boolean

  def can_write:Boolean

  def start:Unit

  def stop:Unit

}

object FileConfigStore extends Log

/**
 * <p>
 * A simple ConfigStore implementation which only support one broker configuration
 * stored in an XML file.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class FileConfigStore extends ConfigStore {
  import FileConfigStore._

  object StoredBrokerModel {
    def apply(config:BrokerDTO) = {
      val data = marshall(config)
      new StoredBrokerModel(config.rev, data, 0)
    }
  }
  case class StoredBrokerModel(rev:Int, data:Array[Byte], lastModified:Long)

  var file:File = new File("activemq.xml")

  @volatile
  var latest:StoredBrokerModel = null
  @volatile
  var running = false

  val dispatchQueue = createQueue("config store")

  // can't do blocking IO work on the dispatchQueue :(
  // so... use an executor
  var ioWorker:ExecutorService = null


  def start = {
    ioWorker = Executors.newSingleThreadExecutor
    running = true

    file = file.getCanonicalFile;
    file.getParentFile.mkdir
    // Find the latest rev
    val files = file.getParentFile.listFiles
    val regex = (Pattern.quote(file.getName+".")+"""(\d)$""").r
    var revs:List[Int] = Nil
    if( files!=null ) {
      for( file <- files) {
        file.getName match {
          case regex(ver)=>
            revs ::= Integer.parseInt(ver)
          case _ =>
        }
      }
    }
    revs = revs.sortWith((x,y)=> x < y)

    val last = revs.lastOption.map{ rev=>
      val r = read(rev, fileRev(rev))
      if( !file.exists ) {
        write(r)
      } else {
        val x = read(rev, file)
        if ( can_write && !Arrays.equals(r.data, x.data) ) {
          write(x.copy(rev=x.rev+1))
        } else {
          x
        }
      }
    } getOrElse {
      if( file.exists ) {
        read(1, file)
      } else {
        write(StoredBrokerModel(defaultConfig(1)))
      }
    }

    latest = last
    schedualNextUpdateCheck
  }

  def stop = {
    running = false
    ioWorker.shutdown
  }

  def load(eval:Boolean) = {
    unmarshall(latest.data, eval)
  }

  def can_write:Boolean = file.canWrite

  def store(config:BrokerDTO) = {
    debug("storing broker model: %s ver %d", config.id, config.rev)
    if( latest.rev+1 != config.rev ) {
      debug("update request does not match next revision: %d", latest.rev+1)
      false
    } else {
      latest = write(StoredBrokerModel(config))
      true
    }
  }

  private def fileRev(rev:Int) = new File(file.getParent, file.getName+"."+rev)

  private def schedualNextUpdateCheck:Unit = dispatchQueue.after(1, TimeUnit.SECONDS) {
    if( running ) {
      val lastModified = latest.lastModified
      val latestData = latest.data
      val nextRev = latest.rev+1
      ioWorker {
        try {
          val l = file.lastModified
          if( l != lastModified ) {
            val config = read(nextRev, file)
            if ( !Arrays.equals(latestData, config.data) ) {
              val c = unmarshall(config.data)
              c.rev = config.rev
              store(c)
            } else {
              latest = latest.copy(lastModified = l)
            }
          }
          schedualNextUpdateCheck
        }
        catch {
          case e:Exception =>
          // error reading the file..  could be that someone is
          // in the middle of updating the file.
        }
      }
    }
  }



  private def defaultConfig(rev:Int) = {
    val config = Broker.defaultConfig
    config.rev = rev
    config
  }

  private def read(rev:Int, file: File) ={
    val data = IOHelper.readBytes(file)
    val config = unmarshall(data) // validates the xml
    StoredBrokerModel(rev, data, file.lastModified)
  }

  private  def write(config:StoredBrokerModel) = {
    // write to the files..
    IOHelper.writeBinaryFile(file, config.data)
    IOHelper.writeBinaryFile(fileRev(config.rev), config.data)
    config.copy(lastModified = file.lastModified)
  }


  def unmarshall(in:Array[Byte], evalProps:Boolean=false) = {
    if (evalProps) {

      val props = new Properties(System.getProperties)
      val prop_file = file.getParentFile / (file.getName + ".properties")
      if( prop_file.exists() ) {
        FileSupport.using(new FileInputStream(prop_file)) { is=>
          val p = new Properties
          p.load(new FileInputStream(prop_file))
          props.putAll( EncryptionSupport.decrypt( p ))
        }
      }

      unmarshalBrokerDTO(new ByteArrayInputStream(in), props)
    } else {
      unmarshalBrokerDTO(new ByteArrayInputStream(in))
    }
  }

  def marshall(in:BrokerDTO) = {
    val baos = new ByteArrayOutputStream
    marshalBrokerDTO(in, baos, true)
    baos.toByteArray
  }
}
