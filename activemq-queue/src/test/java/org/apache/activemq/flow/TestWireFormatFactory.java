package org.apache.activemq.flow;

import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.wireformat.ObjectStreamWireFormat;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.activemq.wireformat.WireFormatFactory;

public class TestWireFormatFactory implements WireFormatFactory {

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
    public boolean matchesWireformatHeader(ByteSequence byteSequence) {
        throw new UnsupportedOperationException();
    }

    /* (non-Javadoc)
     * @see org.apache.activemq.wireformat.WireFormatFactory#maxWireformatHeaderLength()
     */
    public int maxWireformatHeaderLength() {
        throw new UnsupportedOperationException();
    }
}
