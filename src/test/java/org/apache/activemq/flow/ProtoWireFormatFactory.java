package org.apache.activemq.flow;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.activemq.flow.Commands.Destination;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.activemq.wireformat.WireFormatFactory;

public class ProtoWireFormatFactory implements WireFormatFactory {

    static public class TestWireFormat implements WireFormat {

        public void marshal(Object value, DataOutput out) throws IOException {
            if( value.getClass() == Message.class ) {
                out.writeByte(0);
                ((Message)value).getProto().writeFramed((OutputStream)out);
            } else if( value.getClass() == String.class ) {
                out.writeByte(1);
                out.writeUTF((String) value);
            } else if( value.getClass() == Destination.class ) {
                out.writeByte(2);
                ((Destination)value).writeFramed((OutputStream)out);
            } else {
                throw new IOException("Unsupported type: "+value.getClass());
            }
        }

        public Object unmarshal(DataInput in) throws IOException {
            byte type = in.readByte();
            switch(type) {
                case 0:
                    Commands.Message m = new Commands.Message();
                    m.mergeFramed((InputStream)in);
                    return new Message(m);
                case 1:
                    return in.readUTF();
                case 2:
                    Destination d = new Destination();
                    d.mergeFramed((InputStream)in);
                    return d;
                default:
                    throw new IOException("Unknonw type byte: ");
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
