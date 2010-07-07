package org.apache.activemq.apollo.transport;

import org.fusesource.hawtbuf.Buffer;

public class ObjectStreamProtocolCodecFactory implements ProtocolCodecFactory {

	public ProtocolCodec createProtocolCodec() {
		return new ObjectStreamProtocolCodec();
	}	

    public boolean isIdentifiable() {
        return false;
    }

    public boolean matchesIdentification(Buffer byteSequence) {
        throw new UnsupportedOperationException();
    }

    public int maxIdentificaionLength() {
        throw new UnsupportedOperationException();
    }
}
