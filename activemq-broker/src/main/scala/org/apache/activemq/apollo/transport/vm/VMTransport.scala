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
package org.apache.activemq.apollo.transport.vm

import _root_.java.io.IOException
import _root_.java.net.URI
import _root_.java.util.concurrent.atomic.AtomicBoolean
import _root_.java.util.concurrent.atomic.AtomicInteger

import _root_.org.apache.activemq.apollo.broker._
import _root_.org.apache.activemq.transport.Transport
import _root_.org.apache.activemq.transport.TransportFactory
import _root_.org.apache.activemq.transport.TransportServer
import _root_.org.apache.activemq.transport.pipe.PipeTransport
import _root_.org.apache.activemq.transport.pipe.PipeTransportFactory
import _root_.org.apache.activemq.transport.pipe.PipeTransportServer
import _root_.org.apache.activemq.util.IOExceptionSupport
import _root_.org.apache.activemq.util.URISupport
import _root_.org.apache.activemq.transport.TransportFactorySupport.configure
import _root_.org.apache.activemq.transport.TransportFactorySupport.verify

import _root_.scala.collection.JavaConversions._

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object VMTransportFactory extends Log {
  val DEFAULT_PIPE_NAME = BrokerConstants.DEFAULT_VIRTUAL_HOST_NAME.toString();
}

/**
 * Implements the vm transport which behaves like the pipe transport except that
 * it can start embedded brokers up on demand.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class VMTransportFactory extends PipeTransportFactory with Logging {

  import PipeTransportFactory._
  import VMTransportFactory._
  override protected def log = VMTransportFactory

	/**
	 * This extension of the PipeTransportServer shuts down the broker
	 * when all the connections are disconnected.
	 *
	 * @author chirino
	 */
	class VmTransportServer extends PipeTransportServer {

		val refs = new AtomicInteger()
		var broker:Broker = null

		override def createClientTransport():PipeTransport = {
			refs.incrementAndGet();
			new PipeTransport(this) {

				val stopped = new AtomicBoolean()

				override def stop() = {
					if( stopped.compareAndSet(false, true) ) {
						super.stop();
						if( refs.decrementAndGet() == 0 ) {
							stopBroker();
						}
					}
				}
			};
		}

		def setBroker(broker:Broker) = {
			this.broker = broker;
		}

		def stopBroker() = {
			try {
				this.broker.stop();
				unbind(this);
			} catch {
        case e:Exception=>
				error("Failed to stop the broker gracefully: "+e);
				debug("Failed to stop the broker gracefully: ", e);
			}
		}
	}

  override def bind(uri:String):TransportServer = {
    new VmTransportServer();
  }

  override def connect(location:String):Transport = {
		try {
      var uri = new URI(location)
			var brokerURI:String = null;
			var create = true;
			var name = uri.getHost();
			if (name == null) {
				name = DEFAULT_PIPE_NAME;
			}

			var options = URISupport.parseParamters(uri);
			var config = options.remove("broker").asInstanceOf[String]
			if (config != null) {
				brokerURI = config;
			}
			if ("false".equals(options.remove("create"))) {
				create = false;
			}


			var server = servers.get(name);
			if (server == null && create) {

				// Create the broker on demand.
				var broker = if( brokerURI == null ) {
					new Broker()
				} else {
					BrokerFactory.createBroker(brokerURI);
				}

				// Remove the existing pipe severs if the broker is configured with one...  we want to make sure it
				// uses the one we explicitly configure here.
				for (s <- broker.transportServers ) {
					if (s.isInstanceOf[PipeTransportServer] && name == s.asInstanceOf[PipeTransportServer].getName()) {
						broker.transportServers.remove(s);
					}
				}

				// We want to use a vm transport server impl.
				var vmTransportServer = TransportFactory.bind("vm://" + name+"?wireFormat=null").asInstanceOf[VmTransportServer]
				vmTransportServer.setBroker(broker);
				broker.transportServers.add(vmTransportServer);
				broker.start();

				server = servers.get(name);
			}

			if (server == null) {
				throw new IOException("Server is not bound: " + name);
			}

      var transport = server.connect();
      verify( configure(transport, options), options);

		} catch {
//      case e:URISyntaxException=>
//  			throw IOExceptionSupport.create(e);
      case e:Exception=>
  			throw IOExceptionSupport.create(e);
		}
	}

}
