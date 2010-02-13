package org.apache.activemq.amqp.protocol;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.activemq.amqp.protocol.marshaller.AmqpEncodingError;
import org.apache.activemq.amqp.protocol.marshaller.AmqpMarshaller;
import org.apache.activemq.amqp.protocol.types.AmqpFlow;
import org.apache.activemq.amqp.protocol.types.AmqpHandle;
import org.apache.activemq.amqp.protocol.types.AmqpOptions;
import org.apache.activemq.amqp.protocol.types.AmqpSequenceNo;
import org.apache.activemq.amqp.protocol.types.AmqpType;
import org.apache.activemq.amqp.protocol.types.TypeFactory;
import static org.apache.activemq.amqp.protocol.types.TypeFactory.*;

import junit.framework.TestCase;

public class AmqpProtocolTest extends TestCase {

    public void testSequencNumber() throws Exception {
        AmqpSequenceNo val1 = createAmqpSequenceNo(10);
        AmqpSequenceNo val2 = createAmqpSequenceNo(10);
        assertTrue(val1.equals(val2));
    }

    public void testAmqpFlow() throws Exception {
        AmqpFlow flow = createAmqpFlow();
        flow.setHandle(1);
        flow.setOptions(createAmqpOptions());
        flow.getOptions().put(createAmqpString("Hello"), createAmqpUint(20));
        flow.setLimit(2);

        AmqpFlow read = marshalUnmarshal(flow);

        assertTrue(flow.equals(read));
        System.out.println("Value: " + read);
    }

    private <T extends AmqpType<?, ?>> T marshalUnmarshal(T type) throws IOException, AmqpEncodingError {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bos);

        AmqpMarshaller marshaller = org.apache.activemq.amqp.protocol.marshaller.v1_0_0.AmqpMarshaller.getMarshaller();
        type.marshal(out, marshaller);
        out.flush();

        DataInputStream in = new DataInputStream(new ByteArrayInputStream(bos.toByteArray()));
        return (T) marshaller.unmarshalType(in);
    }
}
