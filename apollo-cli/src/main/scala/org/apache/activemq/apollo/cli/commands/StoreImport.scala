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
import io.airlift.command.{Command, Option, Arguments}
import org.apache.activemq.apollo.util.FileSupport._
import org.apache.activemq.apollo.dto.VirtualHostDTO
import org.apache.activemq.apollo.util._
import org.apache.activemq.apollo.broker.ConfigStore
import org.apache.activemq.apollo.broker.store.StoreFactory
import java.io._
import org.apache.activemq.apollo.util.Failure


/**
 * The apollo stop command
 */
@Command(name = "store-import", description = "imports a previously exported message store")
class StoreImport extends BaseAction {

  object StoreImport extends Log

  @Option(name = Array("--conf"), description = "The Apollo configuration file.")
  var conf: File = _

  @Option(name = Array("--virtual-host"), description = "The id of the virtual host to import into, if not specified, the default virtual host is selected.")
  var host: String = _

  @Arguments(description = "The compressed tar file the contains that data for the import", required=true)
  var file:File = _

  def execute(in: InputStream, out: PrintStream, err: PrintStream): Int = {
    init_logging
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

      out.println("Importing: "+file)
      using( new BufferedInputStream(new FileInputStream(file)) ) { is =>
        sync_cb[scala.Option[String]] { cb =>
          store.import_data(is, cb)
        }.foreach(error _)
      }

      ServiceControl.stop(store, "store stop");
      out.println("Done.")
      0
    } catch {
      case x:Helper.Failure=>
        error(x.getMessage)
        1
    }
  }


}