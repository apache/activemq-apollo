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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;

import org.apache.activemq.util.buffer.AsciiBuffer;

import static org.apache.activemq.apollo.stomp.StompWireFormat.*;
import static org.apache.activemq.util.buffer.AsciiBuffer.*;
import static org.apache.activemq.util.buffer.UTF8Buffer.*;


public class StompConnection {

    private Socket socket;
    private BufferedInputStream is;
    private BufferedOutputStream os;
    
    public void open(String host, int port) throws IOException, UnknownHostException {
        open(new Socket(host, port));
    }
    
    public void open(Socket socket) throws IOException {
    	this.socket = socket;
    	this.is = new BufferedInputStream(socket.getInputStream());
        this.os = new BufferedOutputStream(socket.getOutputStream());
    }

    public void close() throws IOException {
        if (socket != null) {
            socket.close();
            socket = null;
        }
    }

    public void sendFrame(StompFrame frame) throws Exception {
        write(frame, os);
        os.flush();
    }
    
    public StompFrame receive() throws Exception {
        return read(is);
    }    
    
	public Socket getStompSocket() {
		return socket;
	}

	public void setStompSocket(Socket stompSocket) {
		this.socket = stompSocket;
	}
	
    public void connect(String username, String password) throws Exception {
        connect(username, password, null);
    }
	
    public void connect(String username, String password, String client) throws Exception {
    	HashMap<AsciiBuffer, AsciiBuffer> headers = new HashMap<AsciiBuffer, AsciiBuffer>();
    	headers.put(Stomp.Headers.Connect.LOGIN, ascii(username));
    	headers.put(Stomp.Headers.Connect.PASSCODE, ascii(password));
    	if (client != null) {
    		headers.put(Stomp.Headers.Connect.CLIENT_ID, ascii(client));
    	}
    	StompFrame frame = new StompFrame(Stomp.Commands.CONNECT, headers);
        sendFrame(frame);
        
        StompFrame connect = receive();
        if (!connect.getAction().equals(Stomp.Responses.CONNECTED)) {
        	throw new Exception ("Not connected: " + utf8(connect.getContent()));
        }
    }
    
    public void disconnect() throws Exception {
    	StompFrame frame = new StompFrame(Stomp.Commands.DISCONNECT);
        sendFrame(frame);
    }
    
    public void send(String destination, String message) throws Exception {
    	send(destination, message, null, null);
    }
    
    public void send(String destination, String message, String transaction, HashMap<AsciiBuffer, AsciiBuffer> headers) throws Exception {
    	if (headers == null) {
    		headers = new HashMap<AsciiBuffer, AsciiBuffer>();
    	}
    	headers.put(Stomp.Headers.Send.DESTINATION, ascii(destination));
    	if (transaction != null) {
    		headers.put(Stomp.Headers.TRANSACTION, ascii(transaction));
    	}
    	StompFrame frame = new StompFrame(Stomp.Commands.SEND, headers, utf8(message));
        sendFrame(frame);
    }
    
    public void subscribe(String destination) throws Exception {
    	subscribe(destination, null, null);
    }
    
    public void subscribe(String destination, String ack) throws Exception {
    	subscribe(destination, ack, new HashMap<AsciiBuffer, AsciiBuffer>());
    }
    
    public void subscribe(String destination, String ack, HashMap<AsciiBuffer, AsciiBuffer> headers) throws Exception {
		if (headers == null) {
			headers = new HashMap<AsciiBuffer, AsciiBuffer>();
		}
		headers.put(Stomp.Headers.Subscribe.DESTINATION, ascii(destination));
    	if (ack != null) {
    		headers.put(Stomp.Headers.Subscribe.ACK_MODE, ascii(ack));
    	}
    	StompFrame frame = new StompFrame(Stomp.Commands.SUBSCRIBE, headers);
        sendFrame(frame);
    }
    
    public void unsubscribe(String destination) throws Exception {
    	unsubscribe(destination, null);
    }
    
    public void unsubscribe(String destination, HashMap<AsciiBuffer, AsciiBuffer> headers) throws Exception {
		if (headers == null) {
			headers = new HashMap<AsciiBuffer, AsciiBuffer>();
		}
		headers.put(Stomp.Headers.Unsubscribe.DESTINATION, ascii(destination));
    	StompFrame frame = new StompFrame(Stomp.Commands.UNSUBSCRIBE, headers);
        sendFrame(frame);
    }    
    
    public void begin(String transaction) throws Exception {
    	HashMap<AsciiBuffer, AsciiBuffer> headers = new HashMap<AsciiBuffer, AsciiBuffer>();
    	headers.put(Stomp.Headers.TRANSACTION, ascii(transaction));
    	StompFrame frame = new StompFrame(Stomp.Commands.BEGIN, headers);
        sendFrame(frame);
    }
    
    public void abort(String transaction) throws Exception {
    	HashMap<AsciiBuffer, AsciiBuffer> headers = new HashMap<AsciiBuffer, AsciiBuffer>();
    	headers.put(Stomp.Headers.TRANSACTION, ascii(transaction));
    	StompFrame frame = new StompFrame(Stomp.Commands.ABORT, headers);
        sendFrame(frame);
    }
    
    public void commit(String transaction) throws Exception {
    	HashMap<AsciiBuffer, AsciiBuffer> headers = new HashMap<AsciiBuffer, AsciiBuffer>();
    	headers.put(Stomp.Headers.TRANSACTION, ascii(transaction));
    	StompFrame frame = new StompFrame(Stomp.Commands.COMMIT, headers);
    	sendFrame(frame);
    }
    
    public void ack(StompFrame frame) throws Exception {
    	ack(frame.get(Stomp.Headers.Ack.MESSAGE_ID), null);
    }    
    
    public void ack(StompFrame frame, String transaction) throws Exception {
    	ack(frame.get(Stomp.Headers.Ack.MESSAGE_ID), transaction);
    }
    
    public void ack(String messageId) throws Exception {
    	ack(messageId, null);
    }
    
    public void ack(String messageId, String transaction) throws Exception {
        ack(ascii(messageId), transaction);
    }
    
    private void ack(AsciiBuffer messageId, String transaction) throws Exception {
    	HashMap<AsciiBuffer, AsciiBuffer> headers = new HashMap<AsciiBuffer, AsciiBuffer>();
    	headers.put(Stomp.Headers.Ack.MESSAGE_ID, messageId);
    	if (transaction != null)
    		headers.put(Stomp.Headers.TRANSACTION, ascii(transaction));
    	StompFrame frame = new StompFrame(Stomp.Commands.ACK, headers);
    	sendFrame(frame);	
    }

}
