package org.apache.activemq.flow;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import org.apache.activemq.flow.Commands.Destination;
import org.apache.activemq.flow.Commands.FlowControl;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.wireformat.StatefulWireFormat;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.activemq.wireformat.WireFormatFactory;

public class ProtoWireFormatFactory implements WireFormatFactory {

    public class TestWireFormat implements StatefulWireFormat {
        private ByteBuffer currentOut;
        private byte outType;
        
        private ByteBuffer currentIn;
        private byte inType;
        
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
            }else if( value.getClass() == FlowControl.class ) {
                out.writeByte(3);
                ((FlowControl)value).writeFramed((OutputStream)out);
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
                case 3:
                    FlowControl fc = new FlowControl();
                    fc.mergeFramed((InputStream)in);
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
                	
                	currentOut = ByteBuffer.wrap(((Message)value).getProto().toFramedByteArray());
                	outType = 0;
                } else if( value.getClass() == String.class ) {
                	outType = 1;
                    try {
                        currentOut = ByteBuffer.wrap(((String)value).getBytes("utf-8"));
                    } catch (UnsupportedEncodingException e) {
                        //Shouldn't happen.
                        throw IOExceptionSupport.create(e);
                    }
                } else if( value.getClass() == Destination.class ) {
                	outType = 2;
                    currentOut = ByteBuffer.wrap(((Destination)value).toFramedByteArray());
                }else if( value.getClass() == FlowControl.class ) {
                	outType = 3;
                    currentOut = ByteBuffer.wrap(((FlowControl)value).toFramedByteArray());
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
            	Commands.Message m = new Commands.Message();
            	try
            	{
            		m.mergeFramed(currentIn.array());
            	}
            	catch(Exception e)
            	{
            		e.printStackTrace();
            	}
            	ret = new Message(m);
            	break;
            case 1:
            	ret = new String(currentIn.array(), "utf-8");
            	break;
        	case 2:
        		Destination d = new Destination();
        		d.mergeFramed(currentIn.array());
        		ret = d;
        		break;
        	case 3:
        		FlowControl c = new FlowControl();
        		c.mergeFramed(currentIn.array());
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
