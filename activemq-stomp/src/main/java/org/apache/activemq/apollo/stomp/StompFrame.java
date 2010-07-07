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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.activemq.util.buffer.AsciiBuffer;
import org.apache.activemq.util.buffer.Buffer;

/**
 * Represents all the data in a STOMP frame.
 * 
 * @author <a href="http://hiramchirino.com">chirino</a>
 */
public class StompFrame {

    public static final Buffer NO_DATA = new Buffer(0);

    private AsciiBuffer action;
    private Map<AsciiBuffer, AsciiBuffer> headers = new HashMap<AsciiBuffer, AsciiBuffer>();
    private Buffer content = NO_DATA;

    public StompFrame(AsciiBuffer command) {
    	this(command, null, null);
    }
    
    public StompFrame(AsciiBuffer command, Map<AsciiBuffer, AsciiBuffer> headers) {
    	this(command, headers, null);
    }    
    
    public StompFrame(AsciiBuffer command, Map<AsciiBuffer, AsciiBuffer> headers, Buffer data) {
        this.action = command;
        if (headers != null)
        	this.headers = headers;
        if (data != null)
        	this.content = data;
    }
    
    public StompFrame() {
    }

    public AsciiBuffer getAction() {
        return action;
    }

    public void setAction(AsciiBuffer command) {
        this.action = command;
    }

    public Buffer getContent() {
        return content;
    }
    
    public void setContent(Buffer data) {
        this.content = data;
    }

    public AsciiBuffer get(AsciiBuffer header) {
        return headers.get(header);
    }
    
    public AsciiBuffer put(AsciiBuffer key, AsciiBuffer value) {
        return headers.put(key, value);
    }

    public Map<AsciiBuffer, AsciiBuffer> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<AsciiBuffer, AsciiBuffer> headers) {
        this.headers = headers;
    }

    public String toString() {
        StringBuffer buffer = new StringBuffer();
        buffer.append(getAction());
        buffer.append("\n");
        
        for (Entry<AsciiBuffer, AsciiBuffer> entry : headers.entrySet()) {
            buffer.append(entry.getKey());
            buffer.append(":");
            buffer.append(entry.getValue());
            buffer.append("\n");
        }

        buffer.append("\n");
        if (getContent() != null) {
            try {
                buffer.append(getContent());
            } catch (Throwable e) {
            }
        }
        return buffer.toString();
    }

}
