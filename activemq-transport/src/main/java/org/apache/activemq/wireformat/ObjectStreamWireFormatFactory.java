package org.apache.activemq.wireformat;

import org.apache.activemq.util.buffer.Buffer;

public class ObjectStreamWireFormatFactory implements WireFormatFactory {

	public WireFormat createWireFormat() {
		return new ObjectStreamWireFormat();
	}	

	/* (non-Javadoc)
     * @see org.apache.activemq.wireformat.WireFormatFactory#isDiscriminatable()
     */
    public boolean isDiscriminatable() {
        return false;
    }

    /* (non-Javadoc)
     * @see org.apache.activemq.wireformat.WireFormatFactory#matchesWireformatHeader(org.apache.activemq.util.ByteSequence)
     */
    public boolean matchesWireformatHeader(Buffer byteSequence) {
        throw new UnsupportedOperationException();
    }

    /* (non-Javadoc)
     * @see org.apache.activemq.wireformat.WireFormatFactory#maxWireformatHeaderLength()
     */
    public int maxWireformatHeaderLength() {
        throw new UnsupportedOperationException();
    }
}
