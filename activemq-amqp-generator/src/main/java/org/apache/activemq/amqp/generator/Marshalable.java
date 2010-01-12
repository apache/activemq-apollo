package org.apache.activemq.amqp.generator;

import java.io.IOException;
import java.io.OutputStream;

public interface Marshalable {

	public void marshal(OutputStream stream) throws IOException;
}
