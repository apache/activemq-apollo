package org.apache.activemq.flow;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.activemq.wireformat.WireFormatFactory;

public class TestWireFormatFactory implements WireFormatFactory {

    public class TestWireFormat implements WireFormat {

        public void marshal(Object value, DataOutput out) throws IOException {
            ObjectOutputStream oos = new ObjectOutputStream((OutputStream) out);
            oos.writeObject(value);
            oos.reset();
            oos.flush();
        }

        public Object unmarshal(DataInput in) throws IOException {
            ObjectInputStream ois = new ObjectInputStream((InputStream) in);
            try {
                return ois.readObject();
            } catch (ClassNotFoundException e) {
                throw IOExceptionSupport.create(e);
            }
        }

        public int getVersion() {
            return 0;
        }
        public void setVersion(int version) {
        }

        public boolean inReceive() {
            return false;
        }

        public ByteSequence marshal(Object value) throws IOException {
            return null;
        }
        public Object unmarshal(ByteSequence data) throws IOException {
            return null;
        }
    }

	public WireFormat createWireFormat() {
		return new TestWireFormat();
	}

}
