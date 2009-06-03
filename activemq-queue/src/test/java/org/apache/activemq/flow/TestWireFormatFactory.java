package org.apache.activemq.flow;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;

import org.apache.activemq.transport.Transport;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.activemq.wireformat.WireFormatFactory;

public class TestWireFormatFactory implements WireFormatFactory {

    static public class TestWireFormat implements WireFormat {

        public void marshal(Object value, DataOutput out) throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(value);
            oos.close();
            
            byte[] data = baos.toByteArray();
            out.writeInt(data.length);
            out.write(data);
        }

        public Object unmarshal(DataInput in) throws IOException {
            byte data[] = new byte[in.readInt()];
            in.readFully(data);
            
            ByteArrayInputStream is = new ByteArrayInputStream(data);
            ObjectInputStream ois = new ObjectInputStream(is);
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

        public Transport createTransportFilters(Transport transport, Map options) {
           return transport;
        }
    }

	public WireFormat createWireFormat() {
		return new TestWireFormat();
	}	

}
