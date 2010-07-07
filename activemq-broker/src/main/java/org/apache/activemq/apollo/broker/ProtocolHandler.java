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
package org.apache.activemq.apollo.broker;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;

import org.apache.activemq.Service;
import org.apache.activemq.broker.store.Store.MessageRecord;
import org.apache.activemq.filter.BooleanExpression;
import org.apache.activemq.wireformat.WireFormat;

public interface ProtocolHandler extends Service {
    void onCommand(Object command);
    void setConnection(BrokerConnection brokerConnection);

    void setWireFormat(WireFormat wireformat);

    void onException(Exception error);

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
    public interface ConsumerContext { // extends ClientContext, Subscription<MessageDelivery>, IFlowSink<MessageDelivery> {

        public String getConsumerId();

        public Destination getDestination();

        public String getSelector();

        public BooleanExpression getSelectorExpression();

        public boolean isDurable();

        public String getSubscriptionName();

        /**
         * If the destination does not exist, should it automatically be
         * created?
         *
         * @return
         */
        public boolean autoCreateDestination();

        public boolean isPersistent();

    }

}