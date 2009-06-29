package org.apache.activemq.queue.perf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.activemq.flow.Commands.Destination.DestinationBuffer;
import org.apache.activemq.flow.Commands.FlowControl.FlowControlBuffer;
import org.apache.activemq.flow.Commands.Message.MessageBuffer;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.buffer.Buffer;
import org.apache.activemq.util.buffer.DataByteArrayInputStream;
import org.apache.activemq.util.buffer.DataByteArrayOutputStream;
import org.apache.activemq.wireformat.StatefulWireFormat;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.activemq.wireformat.WireFormatFactory;

public class ProtoWireFormatFactory implements WireFormatFactory {

    public class TestWireFormat implements StatefulWireFormat {
        public static final String WIREFORMAT_NAME = "proto";
        
        private ByteBuffer currentOut;
        private byte outType;
        
        private ByteBuffer currentIn;
        private byte inType;
        
        public void marshal(Object value, DataOutput out) throws IOException {
            if( value.getClass() == Message.class ) {
                out.writeByte(0);
                MessageBuffer proto = ((Message)value).getProto();
                Buffer buffer = proto.toUnframedBuffer();
                out.writeInt(buffer.getLength());
                out.write(buffer.getData(), buffer.getOffset(), buffer.getLength());
            } else if( value.getClass() == String.class ) {
                out.writeByte(1);
                String value2 = (String) value;
                byte[] bytes = value2.getBytes("UTF-8");
                out.writeInt(bytes.length);
                out.write(bytes);
            } else if( value.getClass() == DestinationBuffer.class ) {
                out.writeByte(2);
                DestinationBuffer proto = (DestinationBuffer)value;
                Buffer buffer = proto.toUnframedBuffer();
                out.writeInt(buffer.getLength());
                out.write(buffer.getData(), buffer.getOffset(), buffer.getLength());
            }else if( value.getClass() == FlowControlBuffer.class ) {
                out.writeByte(3);
                FlowControlBuffer proto = (FlowControlBuffer)value;
                Buffer buffer = proto.toUnframedBuffer();
                out.writeInt(buffer.getLength());
                out.write(buffer.getData(), buffer.getOffset(), buffer.getLength());
            } else {
                throw new IOException("Unsupported type: "+value.getClass());
            }
        }

        public Object unmarshal(DataInput in) throws IOException {
            byte type = in.readByte();
            int size = in.readInt();
            byte data[] = new byte[size];
            in.readFully(data);
            switch(type) {
                case 0:
                    MessageBuffer m = MessageBuffer.parseUnframed(data);
                    return new Message(m);
                case 1:
                    return new String(data, "UTF-8");
                case 2:
                    DestinationBuffer d = DestinationBuffer.parseUnframed(data);
                    return d;
                case 3:
                    FlowControlBuffer fc = FlowControlBuffer.parseUnframed(data);
                    return fc;
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
                	
                	currentOut = ByteBuffer.wrap(((Message)value).getProto().toUnframedByteArray());
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
                    currentOut = ByteBuffer.wrap(((DestinationBuffer)value).toUnframedByteArray());
                }else if( value.getClass() == FlowControlBuffer.class ) {
                	outType = 3;
                    currentOut = ByteBuffer.wrap(((FlowControlBuffer)value).toUnframedByteArray());
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
            case 0:
            	MessageBuffer m = MessageBuffer.parseUnframed(currentIn.array());
            	ret = new Message(m);
            	break;
            case 1:
            	ret = new String(currentIn.array(), "utf-8");
            	break;
        	case 2:
        		DestinationBuffer d = DestinationBuffer.parseUnframed(currentIn.array());
        		ret = d;
        		break;
        	case 3:
        		FlowControlBuffer c = FlowControlBuffer.parseUnframed(currentIn.array());
        		ret = c;
        		break;
        	default:
        		throw new IOException("Unknown type byte: " + inType);
            }
            
            currentIn = null;
            return ret;
        }
        
        public int getVersion() {
            return 0;
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

        public String getName() {
            return WIREFORMAT_NAME;
        }

		public WireFormatFactory getWireFormatFactory() {
			return new ProtoWireFormatFactory();
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
