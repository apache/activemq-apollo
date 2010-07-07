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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.activemq.transport.Transport;
import org.apache.activemq.util.buffer.AsciiBuffer;
import org.apache.activemq.util.buffer.Buffer;
import org.apache.activemq.util.buffer.ByteArrayInputStream;
import org.apache.activemq.util.buffer.ByteArrayOutputStream;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.activemq.wireformat.WireFormatFactory;

import static org.apache.activemq.apollo.stomp.Stomp.*;
import static org.apache.activemq.apollo.stomp.Stomp.Headers.*;
import static org.apache.activemq.util.buffer.AsciiBuffer.*;

/**
 * Implements marshalling and unmarsalling the <a
 * href="http://activemq.apache.org/stomp/">Stomp</a> protocol.
 */
public class StompWireFormat implements WireFormat {
    
    public static final StompWireFormat INSTANCE = new StompWireFormat();
    
    private static final int MAX_COMMAND_LENGTH = 1024;
    private static final int MAX_HEADER_LENGTH = 1024 * 10;
    private static final int MAX_HEADERS = 1000;
    private static final int MAX_DATA_LENGTH = 1024 * 1024 * 100;
    private static final boolean TRIM=false;

    private int version = 1;
    public static final String WIREFORMAT_NAME = "stomp";

    public Buffer marshal(Object command) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        marshal(command, dos);
        dos.close();
        return baos.toBuffer();
    }

    public Object unmarshal(Buffer packet) throws IOException {
        return read(new ByteArrayInputStream(packet));
    }

    public void marshal(Object command, DataOutput os) throws IOException {
        write((StompFrame) command, (OutputStream) os);
    }

    public Object unmarshal(DataInput in) throws IOException {
        return read((InputStream)in);
    }
    
    public int getVersion() {
        return version;
    }

    public String getName() {
        return WIREFORMAT_NAME;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public boolean inReceive() {
        //TODO implement the inactivity monitor
        return false;
    }

    @SuppressWarnings("unchecked")
    public Transport createTransportFilters(Transport transport, Map options) {
//        if (transport.isUseInactivityMonitor()) {
//            transport = new InactivityMonitor(transport, this);
//        }
        return transport;
    }

	public WireFormatFactory getWireFormatFactory() {
		return new StompWireFormatFactory();
	}

    static public void write(StompFrame stomp, OutputStream os) throws IOException {
        write(os, stomp.getAction());
        os.write(NEWLINE);
        Set<Entry<AsciiBuffer, AsciiBuffer>> entrySet = stomp.getHeaders().entrySet();
        for (Entry<AsciiBuffer, AsciiBuffer> entry : entrySet) {
            AsciiBuffer key = entry.getKey();
            AsciiBuffer value = entry.getValue();
            write(os, key);
            os.write(SEPERATOR);
            write(os, value);
            os.write(NEWLINE);
        }
        os.write(NEWLINE);
        write(os, stomp.getContent());
        write(os, END_OF_FRAME_BUFFER);
    }

    private static void write(OutputStream os, Buffer action) throws IOException {
        os.write(action.data, action.offset, action.length);
    }
    
    static public StompFrame read(InputStream in) throws IOException {

            Buffer action = null;

            // skip white space to next real action line
            while (true) {
                action = readLine(in, MAX_COMMAND_LENGTH, "The maximum command length was exceeded");
                if( TRIM ) {
                    action = action.trim();
                }
                if (action.length() > 0) {
                    break;
                }
            }

            // Parse the headers
            HashMap<AsciiBuffer, AsciiBuffer> headers = new HashMap<AsciiBuffer, AsciiBuffer>(16);
            while (true) {
                Buffer line = readLine(in, MAX_HEADER_LENGTH, "The maximum header length was exceeded");
                if (line != null && line.trim().length() > 0) {

                    if (headers.size() > MAX_HEADERS) {
                        throw new IOException("The maximum number of headers was exceeded");
                    }

                    try {
                        int seperatorIndex = line.indexOf(SEPERATOR);
                        if( seperatorIndex<0 ) {
                            throw new IOException("Header line missing seperator [" + ascii(line) + "]");
                        }
                        Buffer name = line.slice(0, seperatorIndex);
                        if( TRIM ) {
                            name = name.trim();
                        }
                        Buffer value = line.slice(seperatorIndex + 1, line.length());
                        if( TRIM ) {
                            value = value.trim();
                        }
                        headers.put(ascii(name), ascii(value));
                    } catch (Exception e) {
                        throw new IOException("Unable to parser header line [" + line + "]");
                    }
                } else {
                    break;
                }
            }

            // Read in the data part.
            Buffer content = EMPTY_BUFFER;
            AsciiBuffer contentLength = headers.get(CONTENT_LENGTH);
            if (contentLength != null) {

                // Bless the client, he's telling us how much data to read in.
                int length;
                try {
                    length = Integer.parseInt(contentLength.trim().toString());
                } catch (NumberFormatException e) {
                    throw new IOException("Specified content-length is not a valid integer");
                }

                if (length > MAX_DATA_LENGTH) {
                    throw new IOException("The maximum data length was exceeded");
                }

                content = new Buffer(length);
                int pos = 0;
                while( pos < length ) {
                    int rc = in.read(content.data, pos, length-pos);
                    if( rc < 0 ) {
                        throw new IOException("EOF reached before fully reading the content");
                    }
                    pos+=rc; 
                }

                if (in.read() != 0) {
                    throw new IOException("content-length bytes were read and there was no trailing null byte");
                }

            } else {

                // We don't know how much to read.. data ends when we hit a 0
                int b;
                ByteArrayOutputStream baos = null;
                while (true) {
                    b = in.read();
                    if( b < 0 ) {
                        throw new IOException("EOF reached before fully reading the content");
                    }
                    if( b==0 ) {
                        break;
                    }
                    if (baos == null) {
                        baos = new ByteArrayOutputStream();
                    } else if (baos.size() > MAX_DATA_LENGTH) {
                        throw new IOException("The maximum data length was exceeded");
                    }

                    baos.write(b);
                }

                if (baos != null) {
                    baos.close();
                    content = baos.toBuffer();
                }

            }

            return new StompFrame(ascii(action), headers, content);



    }    
    
    static private Buffer readLine(InputStream in, int maxLength, String errorMessage) throws IOException {
        int b;
        ByteArrayOutputStream baos = new ByteArrayOutputStream(40);
        while (true) {
            b = in.read();
            if( b < 0) {
                throw new EOFException("peer closed the connection");
            }
            if( b=='\n') {
                break;
            }
            if (baos.size() > maxLength) {
                throw new IOException(errorMessage);
            }
            baos.write(b);
        }
        baos.close();
        return baos.toBuffer();
    }
}
