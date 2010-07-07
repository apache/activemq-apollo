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
package org.apache.activemq.apollo.stomp;

import org.apache.activemq.apollo.broker.Destination;
import org.apache.activemq.apollo.broker.Router;
import org.apache.activemq.apollo.broker.Destination.Parser;
import org.apache.activemq.util.buffer.AsciiBuffer;


/**
 * Implements ActiveMQ 4.0 translations
 */
public class DefaultFrameTranslator implements FrameTranslator {
    
    private static final Destination.ParserOptions PARSER_OPTIONS = new Destination.ParserOptions();
    static {
        PARSER_OPTIONS.defaultDomain = Router.QUEUE_DOMAIN;
        PARSER_OPTIONS.queuePrefix = new AsciiBuffer("/queue/");
        PARSER_OPTIONS.topicPrefix = new AsciiBuffer("/topic/");
        PARSER_OPTIONS.tempQueuePrefix = new AsciiBuffer("/remote-temp-queue/");
        PARSER_OPTIONS.tempTopicPrefix = new AsciiBuffer("/remote-temp-topic/");
    }
    
    public Destination convert(AsciiBuffer dest) {
        return Parser.parse(dest, PARSER_OPTIONS);
    }
	
//    public ActiveMQMessage convertToOpenwireMessage(StompProtocolHandler converter, StompFrame command) throws JMSException, ProtocolException {
//        final Map<String, String> headers = command.getHeaders();
//        final ActiveMQMessage msg;
//        if (headers.containsKey(Stomp.Headers.CONTENT_LENGTH)) {
//            headers.remove(Stomp.Headers.CONTENT_LENGTH);
//            ActiveMQBytesMessage bm = new ActiveMQBytesMessage();
//            bm.writeBytes(command.getContent());
//            msg = bm;
//        } else {
//            ActiveMQTextMessage text = new ActiveMQTextMessage();
//            try {
//                text.setText(new String(command.getContent(), "UTF-8"));
//            } catch (Throwable e) {
//                throw new ProtocolException("Text could not bet set: " + e, false, e);
//            }
//            msg = text;
//        }
//        FrameTranslator.Helper.copyStandardHeadersFromFrameToMessage(converter, command, msg, this);
//        return msg;
//    }
//
//    public StompFrame convertFromOpenwireMessage(StompProtocolHandler converter, ActiveMQMessage message) throws IOException, JMSException {
//        StompFrame command = new StompFrame();
//        command.setAction(Stomp.Responses.MESSAGE);
//        Map<String, String> headers = new HashMap<String, String>(25);
//        command.setHeaders(headers);
//
//        FrameTranslator.Helper.copyStandardHeadersFromMessageToFrame(converter, message, command, this);
//
//        if (message.getDataStructureType() == ActiveMQTextMessage.DATA_STRUCTURE_TYPE) {
//
//            ActiveMQTextMessage msg = (ActiveMQTextMessage)message.copy();
//            command.setContent(msg.getText().getBytes("UTF-8"));
//
//        } else if (message.getDataStructureType() == ActiveMQBytesMessage.DATA_STRUCTURE_TYPE) {
//
//            ActiveMQBytesMessage msg = (ActiveMQBytesMessage)message.copy();
//            msg.setReadOnlyBody(true);
//            byte[] data = new byte[(int)msg.getBodyLength()];
//            msg.readBytes(data);
//
//            headers.put(Stomp.Headers.CONTENT_LENGTH, "" + data.length);
//            command.setContent(data);
//        }
//        return command;
//    }
//
//    public String convertFromOpenwireDestination(StompProtocolHandler converter, ActiveMQDestination activeMQDestination) {
//        if (activeMQDestination == null) {
//            return null;
//        }
//        String physicalName = activeMQDestination.getPhysicalName();
//
//        String rc = converter.getCreatedTempDestinationName(activeMQDestination);
//        if( rc!=null ) {
//        	return rc;
//        }
//        
//        StringBuffer buffer = new StringBuffer();
//        if (activeMQDestination.isQueue()) {
//            if (activeMQDestination.isTemporary()) {
//                buffer.append("/remote-temp-queue/");
//            } else {
//                buffer.append("/queue/");
//            }
//        } else {
//            if (activeMQDestination.isTemporary()) {
//                buffer.append("/remote-temp-topic/");
//            } else {
//                buffer.append("/topic/");
//            }
//        }
//        buffer.append(physicalName);
//        return buffer.toString();
//    }
//
//    public ActiveMQDestination convertToOpenwireDestination(StompProtocolHandler converter, String name) throws ProtocolException {
//        if (name == null) {
//            return null;
//        } else if (name.startsWith("/queue/")) {
//            String qName = name.substring("/queue/".length(), name.length());
//            return ActiveMQDestination.createDestination(qName, ActiveMQDestination.QUEUE_TYPE);
//        } else if (name.startsWith("/topic/")) {
//            String tName = name.substring("/topic/".length(), name.length());
//            return ActiveMQDestination.createDestination(tName, ActiveMQDestination.TOPIC_TYPE);
//        } else if (name.startsWith("/remote-temp-queue/")) {
//            String tName = name.substring("/remote-temp-queue/".length(), name.length());
//            return ActiveMQDestination.createDestination(tName, ActiveMQDestination.TEMP_QUEUE_TYPE);
//        } else if (name.startsWith("/remote-temp-topic/")) {
//            String tName = name.substring("/remote-temp-topic/".length(), name.length());
//            return ActiveMQDestination.createDestination(tName, ActiveMQDestination.TEMP_TOPIC_TYPE);
//        } else if (name.startsWith("/temp-queue/")) {
//            return converter.createTempQueue(name);
//        } else if (name.startsWith("/temp-topic/")) {
//            return converter.createTempTopic(name);
//        } else {
//            throw new ProtocolException("Illegal destination name: [" + name + "] -- ActiveMQ STOMP destinations "
//                                        + "must begine with one of: /queue/ /topic/ /temp-queue/ /temp-topic/");
//        }
//    }
//
//    public String convertFromDestination(StompProtocolHandler converter, Destination d) throws ProtocolException {
//        if (d == null) {
//            return null;
//        }
//        
//        StringBuffer buffer = new StringBuffer();
//        if( d.getDomain().equals(Router.QUEUE_DOMAIN) ) {
//            buffer.append("/queue/");
//        } else if( d.getDomain().equals(Router.QUEUE_DOMAIN) ) {
//            buffer.append("/topic/");
//        } else {
//            throw new ProtocolException("Illegal destination: Stomp can only handle queue or topic Domains");
//        }
//        
//        buffer.append(d.getName().toString());
//        return buffer.toString();
//    }
//
//    public Destination convertToDestination(StompProtocolHandler converter, String name) throws ProtocolException {
//        if (name == null) {
//            return null;
//        } else if (name.startsWith("/queue/")) {
//            String qName = name.substring("/queue/".length(), name.length());
//            return new Destination.SingleDestination(Router.QUEUE_DOMAIN, new AsciiBuffer(qName));
//        } else if (name.startsWith("/topic/")) {
//            String tName = name.substring("/topic/".length(), name.length());
//            return new Destination.SingleDestination(Router.TOPIC_DOMAIN, new AsciiBuffer(tName));
//        } else if (name.startsWith("/remote-temp-queue/")) {
//            throw new UnsupportedOperationException();
//        } else if (name.startsWith("/remote-temp-topic/")) {
//            throw new UnsupportedOperationException();
//        } else if (name.startsWith("/temp-queue/")) {
//            throw new UnsupportedOperationException();
//        } else if (name.startsWith("/temp-topic/")) {
//            throw new UnsupportedOperationException();
//        } else {
//            throw new ProtocolException("Illegal destination name: [" + name + "] -- ActiveMQ STOMP destinations "
//                                        + "must begine with one of: /queue/ /topic/ /temp-queue/ /temp-topic/");
//        }
//    }
}
