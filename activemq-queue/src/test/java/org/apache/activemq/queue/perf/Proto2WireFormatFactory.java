package org.apache.activemq.queue.perf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.activemq.flow.Commands.Destination.DestinationBean;
import org.apache.activemq.flow.Commands.Destination.DestinationBuffer;
import org.apache.activemq.flow.Commands.FlowControl.FlowControlBean;
import org.apache.activemq.flow.Commands.FlowControl.FlowControlBuffer;
import org.apache.activemq.flow.Commands.Message.MessageBean;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.buffer.Buffer;
import org.apache.activemq.util.buffer.DataByteArrayInputStream;
import org.apache.activemq.util.buffer.DataByteArrayOutputStream;
import org.apache.activemq.wireformat.StatefulWireFormat;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.activemq.wireformat.WireFormatFactory;

public class Proto2WireFormatFactory implements WireFormatFactory {

    
    public class TestWireFormat implements StatefulWireFormat {
        public static final String WIREFORMAT_NAME = "proto";

        private ByteBuffer currentOut;
        private byte outType;
        
        private ByteBuffer currentIn;
        private byte inType;
        
        public void marshal(Object value, DataOutput out) throws IOException {
            if( value.getClass() == Message.class ) {
                out.writeByte(0);
                DataByteArrayOutputStream baos = new DataByteArrayOutputStream();
                MessageBean proto = ((Message)value).getProto().copy();
                proto.writeExternal(baos);
                out.writeInt(baos.size());
                out.write(baos.getData(), 0, baos.size());
            } else if( value.getClass() == String.class ) {
                out.writeByte(1);
                String value2 = (String) value;
                byte[] bytes = value2.getBytes("UTF-8");
                out.writeInt(bytes.length);
                out.write(bytes);
            } else if( value.getClass() == DestinationBuffer.class ) {
                out.writeByte(2);
                DataByteArrayOutputStream baos = new DataByteArrayOutputStream();
                DestinationBean proto = ((DestinationBuffer)value).copy();
                proto.writeExternal(baos);
                out.writeInt(baos.size());
                out.write(baos.getData(), 0, baos.size());
            }else if( value.getClass() == FlowControlBuffer.class ) {
                out.writeByte(3);
                DataByteArrayOutputStream baos = new DataByteArrayOutputStream();
                FlowControlBean proto = ((FlowControlBuffer)value).copy();
                proto.writeExternal(baos);
                out.writeInt(baos.size());
                out.write(baos.getData(), 0, baos.size());

            } else {
                throw new IOException("Unsupported type: "+value.getClass());
            }
        }

        public Object unmarshal(DataInput in) throws IOException {
            byte type = in.readByte();
            int size = in.readInt();
            switch(type) {
                case 0: {
                    MessageBean proto = new MessageBean();
                    proto.readExternal(in);
                    return new Message(proto.freeze());
                }
                case 1: {
                    byte data[] = new byte[size];
                    in.readFully(data);
                    return new String(data, "UTF-8");
                } case 2: {
                    DestinationBean proto = new DestinationBean();
                    proto.readExternal(in);
                    return proto.freeze();
                } case 3: {
                    FlowControlBean proto = new FlowControlBean();
                    proto.readExternal(in);
                    return proto.freeze();
                }
                default:
                    throw new IOException("Unknonw type byte: ");
            }
        }

        public boolean marshal(Object value, ByteBuffer target) throws IOException
        {
            if(currentOut == null)
            {
                //Ensure room for type byte and length byte:
                if(target.remaining() < 5)
                {
                    return false;
                }
                
                if( value.getClass() == Message.class ) {
                    DataByteArrayOutputStream baos = new DataByteArrayOutputStream();
                    MessageBean proto = ((Message)value).getProto().copy();
                    proto.writeExternal(baos);
                	currentOut = ByteBuffer.wrap(baos.getData(), 0, baos.size());
                	outType = 0;
                } else if( value.getClass() == String.class ) {
                	outType = 1;
                    try {
                        currentOut = ByteBuffer.wrap(((String)value).getBytes("utf-8"));
                    } catch (UnsupportedEncodingException e) {
                        //Shouldn't happen.
                        throw IOExceptionSupport.create(e);
                    }
                } else if( value.getClass() == DestinationBuffer.class ) {
                	outType = 2;
                    
                	DataByteArrayOutputStream baos = new DataByteArrayOutputStream();
                	DestinationBean proto = ((DestinationBuffer)value).copy();
                    proto.writeExternal(baos);
                    currentOut = ByteBuffer.wrap(baos.getData(), 0, baos.size());
                }else if( value.getClass() == FlowControlBuffer.class ) {
                	outType = 3;
                    DataByteArrayOutputStream baos = new DataByteArrayOutputStream();
                    FlowControlBean proto = ((FlowControlBuffer)value).copy();
                    proto.writeExternal(baos);
                }else {
                    throw new IOException("Unsupported type: "+value.getClass());
                }
                
                //Write type:
                target.put(outType);
                //Write length:
                target.putInt(currentOut.remaining());
                if(currentOut.remaining() > 1024*1024)
                {
                    throw new IOException("Packet exceeded max memory size!");
                }
            }
            
            //Avoid overflow:
            if(currentOut.remaining() > target.remaining())
            {
                int limit = currentOut.limit();
                currentOut.limit(currentOut.position() + target.remaining());
                target.put(currentOut);
                currentOut.limit(limit);
            }
            else
            {
                target.put(currentOut);
            }
            
            if(!currentOut.hasRemaining())
            {
                currentOut = null;
                return true;
            }
            return false;
        }
  
        /**
         * Unmarshals an object. When the object is read it is returned.
         * @param source
         * @return The object when unmarshalled, null otherwise
         */
        public Object unMarshal(ByteBuffer source) throws IOException
        {
            if(currentIn == null)
            {
                if(source.remaining() < 5)
                {
                    return null;
                }
                
                inType = source.get();
                int length = source.getInt();
                if(length > 1024*1024)
                {
                    throw new IOException("Packet exceeded max memory size!");
                }
                currentIn = ByteBuffer.wrap(new byte[length]);
                
            }
            
            if(!source.hasRemaining())
            {
            	return null;
            }
            
            if(source.remaining() > currentIn.remaining())
            {
            	int limit = source.limit();
            	source.limit(source.position() + currentIn.remaining());
            	currentIn.put(source);
            	source.limit(limit);
            }
            else
            {
            	currentIn.put(source);
            }
            
            //If we haven't finished the packet return to get more data:
            if(currentIn.hasRemaining())
            {
            	return null;
            }
            
            Object ret = null;
            switch(inType) {
            case 0: {
                DataByteArrayInputStream in = new DataByteArrayInputStream(currentIn.array());
            	MessageBean proto = new MessageBean();
            	proto.readExternal(in);
            	ret = new Message(proto.freeze());
            	break;
            }
            case 1: {
            	ret = new String(currentIn.array(), "utf-8");
            	break;
            }
        	case 2: {
                DataByteArrayInputStream in = new DataByteArrayInputStream(currentIn.array());
                DestinationBean proto = new DestinationBean();
                proto.readExternal(in);
        		ret = proto.freeze();
        		break;
        	}
        	case 3: {
                DataByteArrayInputStream in = new DataByteArrayInputStream(currentIn.array());
                FlowControlBean proto = new FlowControlBean();
                proto.readExternal(in);
                ret = proto.freeze();
        		break;
        	}
        	default:
        		throw new IOException("Unknown type byte: " + inType);
            }
            
            currentIn = null;
            return ret;
        }
        
        public int getVersion() {
            return 0;
        }
        
        /* (non-Javadoc)
         * @see org.apache.activemq.wireformat.WireFormat#getName()
         */
        public String getName() {
            return WIREFORMAT_NAME;
        }
        
        public void setVersion(int version) {
        }

        public boolean inReceive() {
            return false;
        }

        public Buffer marshal(Object value) throws IOException {
            DataByteArrayOutputStream os = new DataByteArrayOutputStream();
            marshal(value, os);
            return os.toBuffer();
        }
        
        public Object unmarshal(Buffer data) throws IOException {
            DataByteArrayInputStream is = new DataByteArrayInputStream(data);
            return unmarshal(is);
        }
        
        public Transport createTransportFilters(Transport transport, Map options) {
           return transport;
        }

		public WireFormatFactory getWireFormatFactory() {
			return new Proto2WireFormatFactory();
		}
    }

	public WireFormat createWireFormat() {
		return new TestWireFormat();
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
