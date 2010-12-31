package org.apache.activemq.apollo.cli.commands

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
import org.apache.felix.gogo.commands.{Action, Option => option, Argument => argument, Command => command}
import org.osgi.service.command.CommandSession
import org.apache.activemq.apollo.util.FileSupport._
import org.apache.activemq.apollo.util.OptionSupport._
import org.apache.commons.codec.binary.Base64
import java.net.{HttpURLConnection, URL}
import org.apache.activemq.apollo.broker.{VirtualHost, FileConfigStore}
import org.apache.activemq.apollo.dto.{VirtualHostDTO, WebAdminDTO}
import org.apache.activemq.apollo.util.{ServiceControl, Log, DirectBufferPoolFactory, Logging}
import java.util.zip.{ZipEntry, ZipOutputStream, ZipFile}
import org.apache.activemq.apollo.broker.store.{StreamManager, StoreFactory}
import java.io.{OutputStream, FileOutputStream, File}

object StoreExport extends Log

/**
 * The apollo stop command
 */
@command(scope="apollo", name = "store-export", description = "exports the contents of a broker message store")
class StoreExport extends Action {

  @option(name = "--conf", description = "The Apollo configuration file.")
  var conf: File = _

  @option(name = "--virtual-host", description = "The id of the virtual host to export, if not specified, the default virtual host is selected.")
  var host: String = _

  @argument(name = "dest", description = "The destination file to hold the exported data", index=0, required=true)
  var dest:File = _

  def execute(session: CommandSession):AnyRef = {
    import Helper._

    try {

      val base = system_dir("apollo.base")

      if( conf == null ) {
        conf = base / "etc" / "apollo.xml"
      }

      if( !conf.exists ) {
        error("Configuration file'%s' does not exist.\n\nTry creating a broker instance using the 'apollo create' command.".format(conf));
      }

      val config_store = new FileConfigStore
      config_store.file = conf
      config_store.start
      val config = config_store.load(true)

      val hosts = collection.JavaConversions.asScalaIterable(config.virtual_hosts).toArray
      val vho:Option[VirtualHostDTO] = if( host==null ) {
        hosts.headOption
      } else {
        hosts.filter( _.id == host ).headOption
      }

      val vh = vho.getOrElse(error("Could find host to export"))
      if( vh.store == null ) {
        error("The virtual host '%s' does not have a store configured.".format(vh.id))
      }

      val store = StoreFactory.create(vh.store)
      if( store==null ) {
        error("Could not create the store.")
      }

      store.configure(config.store, LoggingReporter(StoreExport))
      ServiceControl.start(store, "store startup")


      using( new ZipOutputStream(new FileOutputStream(dest))) { out=>
        out.setMethod(ZipEntry.DEFLATED)
        out.setLevel(9)
        store.export_pb(new StreamManager[OutputStream]() {
          def entry(name:String, func: (OutputStream) => Unit) = {
            out.putNextEntry(new ZipEntry(name));
            func(out)
            out.closeEntry();
          }
          def using_queue_stream(func: (OutputStream) => Unit) = entry("queues.dat", func)
          def using_queue_entry_stream(func: (OutputStream) => Unit) = entry("queue_entries.dat", func)
          def using_message_stream(func: (OutputStream) => Unit) = entry("messages.dat", func)
        })
      }

      ServiceControl.stop(store, "store stop");

    } catch {
      case x:Failure=>
        error(x.getMessage)
    }
    null
  }


}