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

import _root_.java.beans.ExceptionListener
import _root_.java.io.{IOException}
import _root_.java.util.{LinkedHashMap, HashMap}
import _root_.org.apache.activemq.filter.{BooleanExpression}
import _root_.org.apache.activemq.transport._
import _root_.org.apache.activemq.Service
import _root_.java.lang.{String}
import _root_.org.apache.activemq.util.{FactoryFinder, IOExceptionSupport}
import _root_.org.apache.activemq.wireformat.WireFormat
import _root_.org.fusesource.hawtdispatch.ScalaDispatch._
class ConnectionConfig {

}
abstract class Connection() extends TransportListener with Service {

  val dispatchQueue = createQueue("connection")
  var name = "connection"
  var stopping = false;

  var transport:Transport = null
  var exceptionListener:ExceptionListener = null;

  def start() = ^{
    transport.setDispatchQueue(dispatchQueue);
    transport.setTransportListener(Connection.this);
    transport.start()
  } ->: dispatchQueue

  def stop() = ^{
    stopping=true
    transport.stop()
    dispatchQueue.release
  } ->: dispatchQueue

  def onException(error:IOException) = {
    if (!stopping) {
        onFailure(error);
    }
  }

  def onFailure(error:Exception) = {
    if (exceptionListener != null) {
        exceptionListener.exceptionThrown(error);
    }
  }

  def onDisconnected() = {
  }

  def onConnected() = {
  }

}

object BrokerConnection extends Log

class BrokerConnection(val broker: Broker) extends Connection with Logging {
  override protected def log = BrokerConnection

  var protocolHandler: ProtocolHandler = null;

  exceptionListener = new ExceptionListener() {
    def exceptionThrown(error:Exception) = {
      info("Transport failed before messaging protocol was initialized.", error);
      stop()
    }
  }


  def onCommand(command: Object) = {
    if (protocolHandler != null) {
      protocolHandler.onCommand(command);
    } else {
      try {
        var wireformat:WireFormat = null;

        if (command.isInstanceOf[WireFormat]) {

          // First command might be from the wire format decriminator, letting
          // us know what the actually wireformat is.
          wireformat = command.asInstanceOf[WireFormat];

          try {
            protocolHandler = ProtocolHandlerFactory.createProtocolHandler(wireformat.getName());
          } catch {
            case e:Exception=>
            throw IOExceptionSupport.create("No protocol handler available for: " + wireformat.getName(), e);
          }

          protocolHandler.setConnection(this);
          protocolHandler.setWireFormat(wireformat);
          protocolHandler.start();

          exceptionListener = new ExceptionListener() {
            def exceptionThrown(error:Exception) {
              protocolHandler.onException(error);
            }
          }
          protocolHandler.onCommand(command);

        } else {
          throw new IOException("First command should be a WireFormat");
        }

      } catch {
        case e:Exception =>
        onFailure(e);
      }
    }
  }

  override def stop() = {
    super.stop();
    if (protocolHandler != null) {
      protocolHandler.stop();
    }
  }
}


object ProtocolHandlerFactory {
    val PROTOCOL_HANDLER_FINDER = new FactoryFinder("META-INF/services/org/apache/activemq/broker/protocol/");

    def createProtocolHandler(protocol:String) = {
        PROTOCOL_HANDLER_FINDER.newInstance(protocol).asInstanceOf[ProtocolHandler]
    }
}

trait ProtocolHandler extends Service {

    def onCommand(command:Any);
    def setConnection(brokerConnection:BrokerConnection);
    def setWireFormat(wireformat:WireFormat);
    def onException(error:Exception);

// TODO:
//    public void setConnection(BrokerConnection connection);
//
//    public BrokerConnection getConnection();
//
//    public void onCommand(Object command);
//
//    public void onException(Exception error);
//
//    public void setWireFormat(WireFormat wf);
//
//    public BrokerMessageDelivery createMessageDelivery(MessageRecord record) throws IOException;
//
//    /**
//     * ClientContext
//     * <p>
//     * Description: Base interface describing a channel on a physical
//     * connection.
//     * </p>
//     *
//     * @author cmacnaug
//     * @version 1.0
//     */
//    public interface ClientContext {
//        public ClientContext getParent();
//
//        public Collection<ClientContext> getChildren();
//
//        public void addChild(ClientContext context);
//
//        public void removeChild(ClientContext context);
//
//        public void close();
//
//    }
//
//    public abstract class AbstractClientContext<E extends MessageDelivery> extends AbstractLimitedFlowResource<E> implements ClientContext {
//        protected final HashSet<ClientContext> children = new HashSet<ClientContext>();
//        protected final ClientContext parent;
//        protected boolean closed = false;
//
//        public AbstractClientContext(String name, ClientContext parent) {
//            super(name);
//            this.parent = parent;
//            if (parent != null) {
//                parent.addChild(this);
//            }
//        }
//
//        public ClientContext getParent() {
//            return parent;
//        }
//
//        public void addChild(ClientContext child) {
//            if (!closed) {
//                children.add(child);
//            }
//        }
//
//        public void removeChild(ClientContext child) {
//            if (!closed) {
//                children.remove(child);
//            }
//        }
//
//        public Collection<ClientContext> getChildren() {
//            return children;
//        }
//
//        public void close() {
//
//            closed = true;
//
//            for (ClientContext c : children) {
//                c.close();
//            }
//
//            if (parent != null) {
//                parent.removeChild(this);
//            }
//
//            super.close();
//        }
//    }
//
}

trait ConsumerContext { // extends ClientContext, Subscription<MessageDelivery>, IFlowSink<MessageDelivery> {

    def getConsumerId() : String

    def getDestination(): Destination

    def getSelector() : String

    def getSelectorExpression() : BooleanExpression

    def isDurable() : Boolean

    def getSubscriptionName() : String

    /**
     * If the destination does not exist, should it automatically be
     * created?
     *
     * @return
     */
    def autoCreateDestination():Boolean

    def isPersistent() : Boolean

}

