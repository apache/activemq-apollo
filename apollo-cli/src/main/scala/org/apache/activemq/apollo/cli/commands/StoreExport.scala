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
package org.apache.activemq.apollo.cli.commands


import io.airlift.command.{Arguments, Command, Option}
import org.apache.activemq.apollo.util.FileSupport._
import org.apache.activemq.apollo.dto.VirtualHostDTO
import org.apache.activemq.apollo.util._
import org.apache.activemq.apollo.broker.ConfigStore
import java.io._
import org.apache.activemq.apollo.broker.store.StoreFactory

/**
 * The apollo stop command
 */
@Command(name = "store-export", description = "exports the contents of a broker message store")
class StoreExport extends BaseAction {

  object StoreExport extends Log

  @Option(name = Array("--conf"), description = "The Apollo configuration file.")
  var conf: File = _

  @Option(name = Array("--virtual-host"), description = "The id of the virtual host to export, if not specified, the default virtual host is selected.")
  var host: String = _

  @Arguments(description = "The compressed tar file to hold the exported data", required=true)
  var file:File = _

  def execute(in: InputStream, out: PrintStream, err: PrintStream): Int = {
    init_logging
    import Helper._

    try {

      val base = system_dir("apollo.base")

      if( conf == null ) {
        conf = base / "etc" / "apollo.xml"
      }

      if( !conf.exists ) {
        error("Configuration file'%s' does not exist.\n\nTry creating a broker instance using the 'apollo create' command.".format(conf));
      }

      val config = ConfigStore.load(conf, out.println _)

      val hosts = collection.JavaConversions.collectionAsScalaIterable(config.virtual_hosts).toArray
      val vho:scala.Option[VirtualHostDTO] = if( host==null ) {
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

      out.println("Starting store: "+store)
      ServiceControl.start(store, "store startup")

      out.println("Exporting... (this might take a while)")
      using( new BufferedOutputStream(new FileOutputStream(file)) ) { os=>
        sync_cb[scala.Option[String]] { cb =>
          store.export_data(os, cb)
        }.foreach(error _)
      }
      ServiceControl.stop(store, "store stop");
      out.println("Done. Export located at: "+file)
      0
    } catch {
      case x:Failure=>
        error(x.getMessage)
        1
    }
  }

}