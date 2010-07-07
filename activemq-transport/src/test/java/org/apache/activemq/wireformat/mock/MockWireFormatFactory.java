package org.apache.activemq.wireformat.mock;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import org.fusesource.hawtbuf.Buffer;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.activemq.wireformat.WireFormatFactory;

public class MockWireFormatFactory implements WireFormatFactory {

    public class MockWireFormat implements WireFormat {

		public String getName() {
			return "mock";
		}

		public Buffer marshal(Object command) throws IOException {
	        throw new UnsupportedOperationException();
		}

		public void marshal(Object command, DataOutput out) throws IOException {
	        throw new UnsupportedOperationException();
		}

		public Object unmarshal(Buffer packet) throws IOException {
	        throw new UnsupportedOperationException();
		}

		public Object unmarshal(DataInput in) throws IOException {
	        throw new UnsupportedOperationException();
		}

        public void setReadableByteChannel(ReadableByteChannel channel) {
            throw new UnsupportedOperationException();
        }

        public Object read() throws IOException {
            throw new UnsupportedOperationException();
        }

        public void unread(Buffer buffer) {
            throw new UnsupportedOperationException();
        }

        public long getReadCounter() {
            throw new UnsupportedOperationException();
        }

        public void setWritableByteChannel(WritableByteChannel channel) {
            throw new UnsupportedOperationException();
        }

        public BufferState write(Object value) throws IOException {
            throw new UnsupportedOperationException();
        }
        public BufferState flush() throws IOException {
            throw new UnsupportedOperationException();
        }

        public long getWriteCounter() {
            throw new UnsupportedOperationException();
        }
    }

	public WireFormat createWireFormat() {
		return new MockWireFormat();
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
}
