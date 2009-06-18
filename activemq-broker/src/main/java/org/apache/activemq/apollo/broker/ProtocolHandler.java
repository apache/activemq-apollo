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

import org.apache.activemq.Service;
import org.apache.activemq.broker.store.Store.MessageRecord;
import org.apache.activemq.filter.BooleanExpression;
import org.apache.activemq.flow.IFlowSink;
import org.apache.activemq.queue.Subscription;
import org.apache.activemq.wireformat.WireFormat;

public interface ProtocolHandler extends Service {

    public void setConnection(BrokerConnection connection);

    public BrokerConnection getConnection();

    public void onCommand(Object command);

    public void onException(Exception error);

    public void setWireFormat(WireFormat wf);

    public BrokerMessageDelivery createMessageDelivery(MessageRecord record) throws IOException;

    public interface ConsumerContext extends Subscription<MessageDelivery>, IFlowSink<MessageDelivery> {
    	
        public String getConsumerId();
        
        public Destination getDestination();

        public String getSelector();
        
        public BooleanExpression getSelectorExpression();
        
        public boolean isDurable();
        
        public String getSubscriptionName();
        
        /**
         * If the destination does not exist, should it automatically be created? 
         * @return
         */
        public boolean autoCreateDestination();
        
    }

}