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

import org.apache.activemq.apollo.dto.{XmlCodec, BrokerDTO}
import org.fusesource.hawtdispatch._
import java.util.concurrent.{TimeUnit, ExecutorService, Executors}
import org.fusesource.hawtbuf.{ByteArrayInputStream, ByteArrayOutputStream}
import security.EncryptionSupport
import XmlCodec._
import org.apache.activemq.apollo.util._
import java.util.Arrays
import FileSupport._
import java.util.Properties
import java.io.{FileOutputStream, FileInputStream, File}

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

  def read(): String
  def write(value:String): Unit

  def load(eval:Boolean): BrokerDTO

  def store(config:BrokerDTO): Unit

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

  case class StoredBrokerModel(data:Array[Byte], last_modified:Long)

  var file:File = new File("activemq.xml")

  @volatile
  var latest:StoredBrokerModel = null
  @volatile
  var running = false

  val dispatch_queue = createQueue("config store")

  // can't do blocking IO work on the dispatchQueue :(
  // so... use an executor
  var io_worker:ExecutorService = null


  def start = {
    io_worker = Executors.newSingleThreadExecutor
    running = true

    file = file.getCanonicalFile;

    if( !file.exists ) {
      try {
        // try to create a default version of the file.
        store(Broker.defaultConfig)
      } catch {
        case e:Throwable =>
      }
      if( !file.exists ) {
        throw new Exception("The '%s' configuration file does not exist.".format(file.getPath))
      }
    }

    latest = read(file)
    schedual_next_update_check
  }

  def stop = {
    running = false
    io_worker.shutdown
  }

  def load(eval:Boolean) = {
    unmarshall(latest.data, eval)
  }

  def read() = {
    new String(latest.data)
  }

  def can_write:Boolean = file.canWrite

  def store(config:BrokerDTO):Unit = {
    val data = marshall(config)
    latest = write(StoredBrokerModel(data, 0))
  }

  def write(value:String) = {
    val m = StoredBrokerModel(value.getBytes, 0)
    unmarshall(m.data)
    latest = write(m)
  }

  private def schedual_next_update_check:Unit = dispatch_queue.after(1, TimeUnit.SECONDS) {
    if( running ) {
      val last_modified = latest.last_modified
      val latestData = latest.data
      io_worker {
        try {
          val l = file.lastModified
          if( l != last_modified ) {
            val config = read(file)
            if ( !Arrays.equals(latestData, config.data) ) {
              // TODO: trigger reloading the config file.
            }
            latest = config
          }
          schedual_next_update_check
        }
        catch {
          case e:Exception =>
          // error reading the file..  could be that someone is
          // in the middle of updating the file.
        }
      }
    }
  }

  private def read(file: File) ={
    val data = IOHelper.readBytes(file)
    val config = unmarshall(data) // validates the xml
    StoredBrokerModel(data, file.lastModified)
  }

  private  def write(config:StoredBrokerModel) = {

    // backup the config file...
    if(file.exists()) {
      using(new FileInputStream(file)) { in =>
        using(new FileOutputStream(file.getParentFile / ("~"+file.getName))) { out =>
          copy(in, out)
        }
      }
    }

    IOHelper.writeBinaryFile(file, config.data)
    config.copy(last_modified = file.lastModified)
  }


  def unmarshall(in:Array[Byte], evalProps:Boolean=false) = {
    if (evalProps) {

      val props = new Properties()
      props.putAll(System.getProperties)
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
