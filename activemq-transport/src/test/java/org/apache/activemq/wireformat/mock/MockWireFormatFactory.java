package org.apache.activemq.wireformat.mock;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.apache.activemq.transport.Transport;
import org.apache.activemq.util.buffer.Buffer;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.activemq.wireformat.WireFormatFactory;

public class MockWireFormatFactory implements WireFormatFactory {

    public class MockWireFormat implements WireFormat {
		public Transport createTransportFilters(Transport transport, Map options) {
			return transport;
		}

		public String getName() {
			return "mock";
		}

		public int getVersion() {
			return 0;
		}

		public boolean inReceive() {
			return false;
		}

		public void setVersion(int version) {
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

		public WireFormatFactory getWireFormatFactory() {
			return new MockWireFormatFactory();
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
