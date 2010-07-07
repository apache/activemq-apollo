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
package org.apache.activemq.wireformat;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.util.FactoryFinder;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.ByteArrayInputStream;
import org.fusesource.hawtbuf.ByteArrayOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MultiWireFormatFactory implements WireFormatFactory {

    private static final Log LOG = LogFactory.getLog(MultiWireFormatFactory.class);

    private static final FactoryFinder WIREFORMAT_FACTORY_FINDER = new FactoryFinder("META-INF/services/org/apache/activemq/wireformat/");

//    private String wireFormats = "openwire, stomp";
    private String wireFormats = "stomp";
    private ArrayList<WireFormatFactory> wireFormatFactories;

    static class MultiWireFormat implements WireFormat {

        public static final String WIREFORMAT_NAME = "multi";

        ArrayList<WireFormatFactory> wireFormatFactories = new ArrayList<WireFormatFactory>();
        WireFormat wireFormat;
        int maxHeaderLength;
        private ReadableByteChannel readableChannel;
        private ByteBuffer buffer;

        public void setReadableByteChannel(ReadableByteChannel readableChannel) {
            this.readableChannel = readableChannel;
            this.buffer = ByteBuffer.allocate(maxHeaderLength);
        }

        public Object read() throws IOException {
            if( wireFormat!=null ) {
                throw new IOException("Protocol already discriminated.");
            }
            readableChannel.read(buffer);

            Buffer b = new Buffer(buffer.array(), 0, buffer.position());
            for (WireFormatFactory wff : wireFormatFactories) {
                if (wff.matchesWireformatHeader( b )) {
                    wireFormat = wff.createWireFormat();
                    wireFormat.unread(b);
                    return wireFormat;
                }
            }

            if( buffer.position() >= maxHeaderLength ) {
                throw new IOException("Could not discriminate the protocol.");
            }
            return null;
        }

        public void unread(Buffer buffer) {
            throw new UnsupportedOperationException();
        }

        public long getReadCounter() {
            return buffer.position();
        }

        public void setWritableByteChannel(WritableByteChannel writableChannel) {
        }

        public BufferState write(Object value) throws IOException {
            throw new UnsupportedOperationException();
        }

        public BufferState flush() throws IOException {
            throw new UnsupportedOperationException();
        }

        public long getWriteCounter() {
            return 0;
        }


        public void marshal(Object command, DataOutput out) throws IOException {
            throw new UnsupportedOperationException();
        }

        public Buffer marshal(Object command) throws IOException {
            throw new UnsupportedOperationException();
        }

        public Object unmarshal(Buffer packet) throws IOException {
            throw new UnsupportedOperationException();
        }

        public Object unmarshal(DataInput in) throws IOException {
            throw new UnsupportedOperationException();
        }

        public ArrayList<WireFormatFactory> getWireFormatFactories() {
            return wireFormatFactories;
        }

        private void setWireFormatFactories(ArrayList<WireFormatFactory> wireFormatFactories) {
            this.wireFormatFactories = wireFormatFactories;
            maxHeaderLength = 0;
            for (WireFormatFactory wff : wireFormatFactories) {
                maxHeaderLength = Math.max(maxHeaderLength, wff.maxWireformatHeaderLength());
            }
        }

        public String getName() {
            if (wireFormat == null) {
                return WIREFORMAT_NAME;
            } else {
                return wireFormat.getName();
            }
        }
    }

    public MultiWireFormatFactory() {
    }
    
    public MultiWireFormatFactory(List<WireFormatFactory> factories) {
    	setWireFormatFactories(factories);
	}

	public WireFormat createWireFormat() {
        MultiWireFormat rc = new MultiWireFormat();
        if (wireFormatFactories == null) {
            wireFormatFactories = new ArrayList<WireFormatFactory>();
            String[] formats = getWireFormats().split("\\s*\\,\\s*");
            for (int i = 0; i < formats.length; i++) {
                try {
                    WireFormatFactory wff = (WireFormatFactory) WIREFORMAT_FACTORY_FINDER.newInstance(formats[i]);
                    if (wff.isDiscriminatable()) {
                        wireFormatFactories.add(wff);
                    } else {
                        throw new Exception("Not Discriminitable");
                    }
                } catch (Exception e) {
                    LOG.debug("Invalid wireformat '" + formats[i] + "': " + e.getMessage());
                }
            }
        }
        rc.setWireFormatFactories(wireFormatFactories);
        return rc;
    }

    public String getWireFormats() {
        return wireFormats;
    }

    public void setWireFormats(String formats) {
        this.wireFormats = formats;
    }

    public boolean isDiscriminatable() {
        return false;
    }

    public boolean matchesWireformatHeader(Buffer byteSequence) {
        throw new UnsupportedOperationException();
    }

    public int maxWireformatHeaderLength() {
        throw new UnsupportedOperationException();
    }
	public List<WireFormatFactory> getWireFormatFactories() {
		return new ArrayList<WireFormatFactory>(wireFormatFactories);
	}
	public void setWireFormatFactories(List<WireFormatFactory> wireFormatFactories) {
		this.wireFormatFactories = new ArrayList<WireFormatFactory>(wireFormatFactories);
	}


}
