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
package org.apache.activemq.apollo

import broker._
import jaxb.{VirtualHostConfig, PropertiesReader, BrokerConfig}
import java.util.regex.Pattern
import javax.xml.stream.{XMLOutputFactory, XMLInputFactory}
import _root_.org.fusesource.hawtdispatch.ScalaDispatch._
import org.apache.activemq.util.{Hasher, IOHelper}
import java.util.concurrent.{TimeUnit, ExecutorService, Executors}
import org.fusesource.hawtbuf.{ByteArrayInputStream, ByteArrayOutputStream}
import org.apache.activemq.Service
import javax.xml.bind.{Marshaller, JAXBContext}
import java.io.{OutputStreamWriter, File}

/**
 * <p>
 * Defines an interface to access and update persistent broker configurations.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait ConfigStore extends Service {

  def listBrokerConfigs(cb: (List[String]) => Unit):Unit

  def getBrokerConfig(id:String, cb: (Option[BrokerConfig]) => Unit):Unit

  def putBrokerConfig(config:BrokerConfig, cb: (Boolean) => Unit):Unit

  def removeBrokerConfig(id:String, rev:Int, cb: (Boolean) => Unit):Unit

}

/**
 * <p>
 * A simple ConfigStore implementation which only support one broker configuration
 * stored in an XML file.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class FileConfigStore extends ConfigStore with BaseService with Logging {
  override protected def log: Log = FileConfigStore

  object StoredBrokerConfig {
    def apply(rev:Int, config:BrokerConfig) = {
      val data = marshall(config)
      new StoredBrokerConfig(config.id, rev, data, Hasher.JENKINS.hash(data, data.length))
    }
  }
  case class StoredBrokerConfig(id:String, rev:Int, data:Array[Byte], hash:Int)

  val context = JAXBContext.newInstance("org.apache.activemq.apollo.jaxb")
  val unmarshaller = context.createUnmarshaller
  val marshaller = context.createMarshaller
  marshaller.setProperty( Marshaller.JAXB_FORMATTED_OUTPUT, java.lang.Boolean.TRUE )
  val inputfactory = XMLInputFactory.newInstance

  var file:File = new File("activemq.xml")
  var latest:StoredBrokerConfig = null
  var readOnly = false

  val dispatchQueue = createQueue("config store")

  // can't do blocking IO work on the dispatchQueue :(
  // so... use an executor
  var ioWorker:ExecutorService = null

  protected def _start(onCompleted:Runnable) = {
    ioWorker = Executors.newSingleThreadExecutor
    ioWorker.execute(^{
      startup(onCompleted)
    })
  }


  def startup(onCompleted:Runnable) = {

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
        read(rev, fileRev(rev))
      } match {
        case None =>
          if( !file.exists ) {
            if( readOnly ) {
              throw new Exception("file does not exsit: "+file)
            } else {
              write(StoredBrokerConfig(1, defaultConfig))
            }
          } else {
            if( readOnly ) {
              read(1, file)
            } else {
              write(read(1, file))
            }
          }
        case Some(x)=> x
      }

      dispatchQueue {
        latest = last
        schedualNextUpdateCheck
        onCompleted.run
      }
  }
  protected def _stop(onCompleted:Runnable) = {
    ioWorker.submit(^{
      onCompleted.run
    })
    ioWorker.shutdown
  }

  def listBrokerConfigs(cb: (List[String]) => Unit) = callback(cb) {
    List(latest.id)
  } >>: dispatchQueue

  def getBrokerConfig(id:String, cb: (Option[BrokerConfig]) => Unit) = callback(cb) {
    if( latest.id == id ) {
      Some(unmarshall(latest.data))
    } else {
      None
    }
  } >>: dispatchQueue

  def putBrokerConfig(config:BrokerConfig, cb: (Boolean) => Unit) = callback(cb) {
    if( latest.id == config.id && latest.rev==config.rev) {
      config.rev += 1
      latest = write(StoredBrokerConfig(config.rev, config))
      true
    } else {
      false
    }
  } >>: dispatchQueue

  def removeBrokerConfig(id:String, rev:Int, cb: (Boolean) => Unit) = callback(cb) {
    // not supported.
    false
  } >>: dispatchQueue

  private def fileRev(rev:Int) = new File(file.getParent, file.getName+"."+rev)

  private def schedualNextUpdateCheck:Unit = dispatchQueue.after(1, TimeUnit.SECONDS) {
    if( serviceState.isStarted ) {
      ioWorker {
        try {
          val config = read(0, file)
          dispatchQueue {
            if (latest.hash != config.hash) {
              // looks like user has manually edited the file
              // on the file system.. treat that like an update.. so we
              // store the change as a revision.

            }
            schedualNextUpdateCheck
          }
        }
        catch {
          case e:Exception =>
            // error reading the file..
            // TODO: updated it to the latest valid version.
          schedualNextUpdateCheck
        }
      }
    }
  }



  private def defaultConfig() = {
    val host = new VirtualHostConfig
    host.hostNames.add("default")

    val config = new BrokerConfig
    config.id = "default"
    config.transportServers.add("tcp://0.0.0.0:61613?wireFormat=multi")
    config.connectUris.add("tcp://localhost:61613")
    config.virtualHosts.add(host)
    config
  }

  private def read(rev:Int, file: File): StoredBrokerConfig = {
    val data = IOHelper.readBytes(file)
    val config = unmarshall(data) // validates the xml
    val hash = Hasher.JENKINS.hash(data, data.length)
    StoredBrokerConfig(config.id, rev, data, hash)
  }

  private  def write(config:StoredBrokerConfig) = {
    // write to the files..
    IOHelper.writeBinaryFile(file, config.data)
    IOHelper.writeBinaryFile(fileRev(config.rev), config.data)
    config
  }

  def unmarshall(in:Array[Byte]) = {
    val bais = new ByteArrayInputStream(in);
    val reader = inputfactory.createXMLStreamReader(bais)
    val properties = new PropertiesReader(reader)
    unmarshaller.unmarshal(properties).asInstanceOf[BrokerConfig]
  }

  def marshall(in:BrokerConfig) = {
    val baos = new ByteArrayOutputStream
    marshaller.marshal(in, new OutputStreamWriter(baos));
    baos.toByteArray
  }
}

object FileConfigStore extends Log