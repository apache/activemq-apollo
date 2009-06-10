package org.apache.activemq.flow;

import org.apache.activemq.wireformat.ObjectStreamWireFormat;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.activemq.wireformat.WireFormatFactory;

public class TestWireFormatFactory implements WireFormatFactory {

	public WireFormat createWireFormat() {
		return new ObjectStreamWireFormat();
	}	

}
