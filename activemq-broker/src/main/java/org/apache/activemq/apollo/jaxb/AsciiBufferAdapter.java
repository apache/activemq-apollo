package org.apache.activemq.apollo.jaxb;

import javax.xml.bind.annotation.adapters.XmlAdapter;

import org.apache.activemq.util.buffer.AsciiBuffer;

class AsciiBufferAdapter extends XmlAdapter<String, AsciiBuffer> {
	@Override
	public String marshal(AsciiBuffer v) throws Exception {
		return v.toString();
	}
	@Override
	public AsciiBuffer unmarshal(String v) throws Exception {
		return new AsciiBuffer(v);
	}
}
