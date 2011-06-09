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

package org.apache.activemq.apollo.openwire.support.broker.openwire;

public class OpenwireMessageEvaluationContext {
//
//    private Message message;
//
//    public OpenwireMessageEvaluationContext() {
//    }
//    public OpenwireMessageEvaluationContext(Message message) {
//        this.message = message;
//    }
//
//    public Message getMessage() {
//        return message;
//    }
//
//    public void setMessage(Message message) {
//        this.message = message;
//    }
//
//
//    public Expression getPropertyExpression(final String name) {
//        Expression expression = JMS_PROPERTY_EXPRESSIONS.get(name);
//        if( expression == null ) {
//            expression = new Expression() {
//                public Object evaluate(MessageEvaluationContext mc) throws FilterException {
//                    try {
//                        Message message = ((OpenwireMessageEvaluationContext) mc).message;
//                        return message.getProperty(name);
//                    } catch (IOException e) {
//                        throw new FilterException(e);
//                    }
//                }
//            };
//        }
//        return expression;
//    }
//
//    public <T> T getBodyAs(Class<T> type) throws FilterException {
//        try {
//            if( type == String.class ) {
//                if ( message instanceof ActiveMQTextMessage ) {
//                    return type.cast(((ActiveMQTextMessage)message).getText());
//                }
//            }
//            if( type == Buffer.class ) {
//                if ( message instanceof ActiveMQBytesMessage ) {
//                    ActiveMQBytesMessage bm = ((ActiveMQBytesMessage)message);
//                    byte data[] = new byte[(int) bm.getBodyLength()];
//                    bm.readBytes(data);
//                    return type.cast(new Buffer(data));
//                }
//            }
//            return null;
//        } catch (JMSException e) {
//            throw new FilterException(e);
//        }
//    }
//
//    public <T> T getDestination() {
//        return (T) destination;
//    }
//    public Object getLocalConnectionId() {
//        throw new UnsupportedOperationException();
//    }
//    public void setDestination(Object destination) {
//        this.destination = destination;
//    }

}
