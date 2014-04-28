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
package org.apache.activemq.apollo.broker.jetty

import org.fusesource.hawtdispatch._
import org.fusesource.hawtdispatch.transport._
import org.apache.activemq.apollo.broker.{Broker, BrokerAware}
import org.apache.activemq.apollo.broker.transport.TransportFactory
import org.apache.activemq.apollo.util._
import org.eclipse.jetty.server.nio.SelectChannelConnector
import javax.net.ssl.SSLContext
import org.eclipse.jetty.server.ssl.SslSelectChannelConnector
import org.eclipse.jetty.util.thread.ExecutorThreadPool
import javax.servlet.http.{HttpServletResponse, HttpServletRequest}
import org.eclipse.jetty.websocket.{WebSocket, WebSocketServlet}
import org.eclipse.jetty.server.{Connector, Server}
import java.net.{URL, InetSocketAddress, URI}
import java.lang.Class
import scala.reflect.BeanProperty
import java.nio.ByteBuffer
import java.nio.channels._
import scala.collection.mutable.ListBuffer
import java.util.concurrent.{ExecutorService, Executor, ArrayBlockingQueue}
import org.fusesource.hawtdispatch.transport.ProtocolCodec.BufferState
import org.fusesource.hawtbuf.{UTF8Buffer, AsciiBuffer, Buffer}
import java.io.{EOFException, IOException}
import java.security.cert.X509Certificate
import org.apache.activemq.apollo.broker.web.AllowAnyOriginFilter
import org.eclipse.jetty.servlet.{FilterMapping, FilterHolder, ServletHolder, ServletContextHandler}
import org.eclipse.jetty.util.log.Slf4jLog
import java.util
import javax.servlet.DispatcherType

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object WebSocketTransportFactory extends TransportFactory.Provider with Log {

  
  def connect(location: String): Transport = {
    return null
  }

  def bind(location: String): TransportServer = {
    var uri: URI = new URI(location)
    uri.getScheme match {
      case "ws" | "wss" =>
        try {
          WsTransportServer(uri)
        } catch {
          // We might not have jetty on the class path.
          case e:java.lang.NoClassDefFoundError =>
            null
        }
      case _ => null
    }
  }

  case class WsTransportServer(uri: URI) extends WebSocketServlet with BaseService with TransportServer with BrokerAware {

    @BeanProperty
    var dispatchQueue = createQueue()
    @BeanProperty
    var blockingExecutor:Executor = _

    @BeanProperty
    var transportServerListener: TransportServerListener = _
    @BeanProperty
    var binary_transfers = true
    @BeanProperty
    var cors_origin:String = null

    @BeanProperty
    var max_text_message_size: Int = -1

    @BeanProperty
    var max_binary_message_size:Int = -1

    @BeanProperty
    var max_idle_time: Int = -1

    var broker: Broker = _


    def set_broker(value: Broker) = broker = value

    var server: Server = _
    var connector: Connector = _

    def dispatch_queue = dispatchQueue

    def start(on_completed: Runnable):Unit = super.start(new TaskWrapper(on_completed))
    def stop(on_completed: Runnable):Unit = super.stop(new TaskWrapper(on_completed))

    protected def _start(on_completed: Task) = blockingExecutor {
      this.synchronized {

        // Explicitly set the Jetty Log impl to avoid
        // the NPE raised at https://issues.apache.org/jira/browse/APLO-264
        org.eclipse.jetty.util.log.Log.setLog(new Slf4jLog());

        IntrospectionSupport.setProperties(this, URISupport.parseParamters(uri));

        accept_dispatch_queue = dispatchQueue.createQueue("accept: " + uri);

        val prefix = "/" + uri.getPath.stripPrefix("/")
        val scheme = uri.getScheme
        val host = uri.getHost
        var port = uri.getPort
        val options = URISupport.parseParamters(uri);

        scheme match {
          case "ws" =>
            if (port == -1) {
              port = 80
            }
          case "wss" =>
            if (port == -1) {
              port = 443
            }
          case _ => throw new Exception("Invalid bind protocol.")
        }

        connector = scheme match {
          case "ws" => new SelectChannelConnector
          case "wss" =>
            val sslContext = if (broker.key_storage != null) {
              val protocol = "TLS"
              val sslContext = SSLContext.getInstance(protocol)
              sslContext.init(broker.key_storage.create_key_managers, broker.key_storage.create_trust_managers, null)
              sslContext
            } else {
              warn("You are using a transport that expects the broker's key storage to be configured.")
              SSLContext.getDefault
            }
            val connector = new SslSelectChannelConnector
            val ssl_settings = connector.getSslContextFactory;
            ssl_settings.setSslContext(sslContext)
            options.get("client_auth") match {
              case null =>
                ssl_settings.setWantClientAuth(true)
              case "want" =>
                ssl_settings.setWantClientAuth(true)
              case "need" =>
                ssl_settings.setNeedClientAuth(true)
              case "none" =>
              case _ =>
                warn("Invalid setting for the wss protcol 'client_auth' query option.  Please set to one of: want, need, or none")
            }
            connector
        }
        connector.setHost(host)
        connector.setPort(port)

        var context = new ServletContextHandler(ServletContextHandler.NO_SECURITY)
        context.setContextPath(prefix)

        if( cors_origin!=null && !cors_origin.trim().isEmpty ) {
          val ALL = util.EnumSet.allOf(classOf[DispatcherType])
          val origins = cors_origin.split(",").map(_.trim()).toSet
          context.addFilter(new FilterHolder(new AllowAnyOriginFilter(origins)), "/*", ALL)
        }
        context.addServlet(new ServletHolder(this), "/")

        server = new Server
        server.setHandler(context)
        server.setConnectors(Array(connector))
        server.setThreadPool(new ExecutorThreadPool(blockingExecutor.asInstanceOf[ExecutorService]))
        server.start

        on_completed.run
      }
    }

    def _stop(on_complete: Task) = blockingExecutor {
      this.synchronized {
        if (server != null) {
          try {
            server.stop
          } catch {
            case ignore =>
          }
        }
        on_complete.run
      }
    }

    def getBoundAddress = {
      val prefix = "/" + uri.getPath.stripPrefix("/")
      new URI(uri.getScheme + "://" + uri.getHost + ":" + connector.getLocalPort + prefix).toString
    }

    def getSocketAddress = new InetSocketAddress(uri.getHost, connector.getLocalPort)

    val pending_connects = new ArrayBlockingQueue[WebSocketTransport](100)
    var accept_dispatch_queue = dispatchQueue

    def resume() = accept_dispatch_queue.resume()

    def suspend() = accept_dispatch_queue.suspend()

    def fire_accept = accept_dispatch_queue {
      val transport = pending_connects.poll()

      if(max_text_message_size != -1){
        transport.connection.setMaxTextMessageSize(max_text_message_size)
      }
      if(max_binary_message_size != -1){
        transport.connection.setMaxBinaryMessageSize(max_binary_message_size)
      }
      if(max_idle_time != -1){
        transport.connection.setMaxIdleTime(max_idle_time)
      }

      if (transport != null) {
        if (service_state.is_started) {
          transportServerListener.onAccept(transport)
        } else {
          blockingExecutor {
            transport.connection.close();
          }
        }
      }
    }

    override def doGet(req: HttpServletRequest, resp: HttpServletResponse) {
      resp.setContentType("application/json")
      resp.getOutputStream.println("{}");
    }

    def doWebSocketConnect(request: HttpServletRequest, protocol: String) = WebSocketTransport(this, request, protocol)
  }

  /**
   *
   */
  case class WebSocketTransport(server: WsTransportServer, request: HttpServletRequest, protocol: String) 
          extends BaseService with WebSocket.OnTextMessage with WebSocket.OnBinaryMessage with Transport with SecuredSession with ScatteringByteChannel with GatheringByteChannel {

    /////////////////////////////////////////////////////////////////////////
    // Transport interface methods.
    /////////////////////////////////////////////////////////////////////////
    
    def blockingExecutor:Executor = server.blockingExecutor

    def getBlockingExecutor = blockingExecutor
    def setBlockingExecutor(value: Executor) {
    }

    var dispatchQueue = createQueue()

    def getDispatchQueue: DispatchQueue = dispatchQueue
    def setDispatchQueue(queue: DispatchQueue) {
      dispatchQueue = queue
      drain_outbound_events.setTargetQueue(queue);
      inbound_dispatch_queue.setTargetQueue(queue);
    }

    @BeanProperty
    var transportListener: TransportListener = _

    val certificates = request.getAttribute("javax.servlet.request.X509Certificate").asInstanceOf[Array[X509Certificate]]
    def getPeerX509Certificates = certificates

    var protocolCodec: ProtocolCodec = _

    def getProtocolCodec = protocolCodec

    def setProtocolCodec(protocolCodec: ProtocolCodec) = {
      this.protocolCodec = protocolCodec
      if( this.protocolCodec!=null ) {
        this.protocolCodec.setTransport(this)
      }
    }

    def getReadChannel: ReadableByteChannel = this
    def getWriteChannel: WritableByteChannel = this

    def dispatch_queue = dispatchQueue

    def start(on_completed: Runnable):Unit = super.start(new TaskWrapper(on_completed))
    def stop(on_completed: Runnable):Unit = super.stop(new TaskWrapper(on_completed))

    protected def _start(on_completed: Task) = {
      inbound_dispatch_queue.setTargetQueue(dispatchQueue)
      drain_outbound_events.setTargetQueue(dispatchQueue)
      transportListener.onTransportConnected();

      inbound.synchronized {
        inbound_capacity_remaining = 1024*64
        inbound.notify();
      }

      on_completed.run()
    }
  
    protected def _stop(on_completed: Task) = {
      inbound_dispatch_queue.resume()
      outbound_executor {
        // Wakes up any blocked reader thread..
        inbound.synchronized {
          inbound.notify();
        }
        connection.close()
        dispatch_queue {
          protocolCodec = null
          on_completed.run()
          transportListener.onTransportDisconnected()
        }
      }
    }

    def getLocalAddress = new InetSocketAddress(request.getLocalAddr, request.getLocalPort)
    def getRemoteAddress = new InetSocketAddress(request.getRemoteHost, request.getRemotePort)

    def isConnected = connection == null || connection.isOpen
    def isClosed = connection == null

    /////////////////////////////////////////////////////////////////////////
    //
    // WebSocket Lifecycle Callbacks...
    //
    /////////////////////////////////////////////////////////////////////////
    var connection: WebSocket.Connection = null
    var closed: Option[(Int, String)] = None

    def onOpen(connection: WebSocket.Connection): Unit = {
      this.connection = connection
      server.pending_connects.put(this)
      server.fire_accept
    }

    def onClose(closeCode: Int, message: String) = dispatchQueue {
      closed = Some(closeCode, message)
      inbound_dispatch_queue {
        drain_inbound
      }
    }

    /////////////////////////////////////////////////////////////////////////
    //
    // This section handles in the inbound flow of messages
    //
    /////////////////////////////////////////////////////////////////////////
    var first_message = true

    def onMessage(str: String): Unit = {
      if( first_message ) {
        // If the first message the client sends us is a text message then
        // we will use text message when responding to the client.
        binary_transfers = false
        first_message = false
      }
      // Convert string messages to bytes messages..  our codecs just work with bytes..
      var buffer = new UTF8Buffer(str)
      onMessage(buffer.data, buffer.offset, buffer.length)
    }

    var inbound_capacity_remaining = 0;
    val inbound = ListBuffer[Buffer]()

    val inbound_dispatch_queue:DispatchQueue = dispatchQueue.createQueue("inbound queue");

    def resumeRead() = {
      inbound_dispatch_queue.resume()
      inbound_dispatch_queue {
        drain_inbound
      }
    }

    def suspendRead() = inbound_dispatch_queue.suspend()

    def onMessage(data: Array[Byte], offset: Int, length: Int): Unit = {
      if( first_message ) {
        // If the first message the client sends us is a binary message then
        // we will use text message when responding to the client.
        binary_transfers = true
        first_message = false
      }
      inbound.synchronized {
        // flow control check..
        while (inbound_capacity_remaining <= 0 && service_state.is_upward ) {
          inbound.wait();
        }
        inbound_capacity_remaining -= length;
      }
      inbound_dispatch_queue {
        inbound += new Buffer(data, offset, length)
        drain_inbound
      }
    }

    def drainInbound = {
      inbound_dispatch_queue {
        drain_inbound
      }
    }

    def close() {}

    def isOpen = inbound.isEmpty && closed != None

    def read(dest: ByteBuffer): Int = {
      inbound_dispatch_queue.assertExecuting()

      if (inbound.isEmpty && closed != None) {
        return -1
      }

      var rc = 0
      while (dest.hasRemaining && !inbound.isEmpty) {
        val src = inbound.head;
        val len = src.length.min(dest.remaining())
        rc += len
        dest.put(src.data, src.offset, len)
        src.moveHead(len)
        if (src.length == 0) {
          inbound.remove(0)
        }
      }

      blockingExecutor {
        inbound.synchronized {
          inbound_capacity_remaining += rc
          inbound.notify();
        }
      }
      rc
    }

    def read(dsts: Array[ByteBuffer]): Long = read(dsts, 0, dsts.length)

    def read(dsts: Array[ByteBuffer], offset: Int, length: Int): Long = {
      if (offset + length > dsts.length || length < 0 || offset < 0) {
        throw new IndexOutOfBoundsException
      }
      var rc = 0L
      var i: Int = 0
      while (i < length) {
        var dst: ByteBuffer = dsts(offset + i)
        if (dst.hasRemaining) {
          rc += read(dst)
        }
        if (dst.hasRemaining) {
          return rc
        }
        i += 1;
        i
      }
      rc
    }

  
    protected def drain_inbound: Unit = {
      dispatch_queue.assertExecuting()
      try {
        //        var initial = protocolCodec.getReadCounter
        //        while (codec.getReadCounter - initial < codec.getReadBufferSize << 2) {
        while (true) {
          if (!service_state.is_started || inbound_dispatch_queue.isSuspended) {
            return
          }
          var command = protocolCodec.read
          if (command != null) {
            try {
              transportListener.onTransportCommand(command)
            } catch {
              case e: Throwable => {
                transportListener.onTransportFailure(new IOException("Transport listener failure: "+e))
              }
            }
          } else {
            return
          }
        }
        //        yieldSource.merge(1)
      } catch {
        case e: IOException => transportListener.onTransportFailure(e)
      }
    }

    /////////////////////////////////////////////////////////////////////////
    //
    // This section handles in the outbound flow of messages
    //
    /////////////////////////////////////////////////////////////////////////

    def full() = protocolCodec == null || protocolCodec.full();

    def offer(command: AnyRef): Boolean = {
      dispatchQueue.assertExecuting
      try {
        if (!service_state.is_started) {
          // this command gets dropped since it was issued after
          // we were stopped..
          return true;
        }
        protocolCodec.write(command) match {
          case BufferState.FULL =>
            return false
          case _ =>
            drain_outbound_events.merge(1)
            return true
        }
      }
      catch {
        case e: IOException => {
          transportListener.onTransportFailure(e)
          return false
        }
      }
    }

    val drain_outbound_events = Dispatch.createSource(EventAggregators.INTEGER_ADD, dispatchQueue)
    drain_outbound_events.setEventHandler(^ { flush })
    drain_outbound_events.resume

    /**
     *
     */
    def flush: Unit = {
      dispatchQueue.assertExecuting
      if (!service_state.is_started) {
        return
      }
      try {
        protocolCodec.flush
      } catch {
        case e: IOException => {
          transportListener.onTransportFailure(e)
        }
      }
    }
    
    def write(srcs: Array[ByteBuffer]): Long = write(srcs, 0, srcs.length)
    def write(srcs: Array[ByteBuffer], offset: Int, length: Int): Long = {
      if (offset + length > srcs.length || length < 0 || offset < 0) {
        throw new IndexOutOfBoundsException
      }
      var rc: Long = 0
      var i: Int = 0
      while (i < length) {
        var src: ByteBuffer = srcs(offset + i)
        if (src.hasRemaining) {
          rc += write(src)
        }
        if (src.hasRemaining) {
          return rc
        }
        i += 1
      }
      rc
    }

    var outbound_capacity_remaining = 1024 * 64;

    object outbound_executor extends SerialExecutor(blockingExecutor) {
      var outbound_drained = 0
      override def drained  = {
        val amount = outbound_drained
        outbound_drained = 0
        dispatch_queue {
          outbound_capacity_remaining += amount
          flush();
          transportListener.onRefill()
        }
      }
    }

    var binary_transfers = false

    var write_failed = false
    def write(buf: ByteBuffer):Int = {
      dispatchQueue.assertExecuting
      val remaining = buf.remaining()
      if( remaining==0 )
        return 0

      if( outbound_capacity_remaining > 0 ) {
        outbound_capacity_remaining -= remaining;
        var buffer = new Buffer(buf.array(), buf.arrayOffset(), buf.remaining())
        outbound_executor {
          if( service_state.is_starting_or_started || !write_failed) {
            try {
              if (!binary_transfers) {
                connection.sendMessage(buffer.utf8().toString)
              } else {
                connection.sendMessage(buffer.data, buffer.offset, buffer.length)
              }
              outbound_executor.outbound_drained += remaining
            } catch {
              case e:IOException =>
                write_failed = true
                dispatch_queue {
                  transportListener.onTransportFailure(e)
                }
            }
          }
        }
        buf.position(buf.position()+ remaining);
        return remaining

      } else {
        return 0
      }
    }

  }


}
