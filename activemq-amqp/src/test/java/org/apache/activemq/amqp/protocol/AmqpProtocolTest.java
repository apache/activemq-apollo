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

import junit.framework.TestCase;

public class AmqpProtocolTest extends TestCase {

    public void testSequencNumber() throws Exception {
        AmqpSequenceNo val1 = new AmqpSequenceNo.AmqpSequenceNoBean(10L);
        AmqpSequenceNo val2 = new AmqpSequenceNo.AmqpSequenceNoBean(10L);
        assertTrue(val1.equivalent(val2));
        
    }
    
    public void testAmqpFlow() throws Exception {
        AmqpFlow val = new AmqpFlow.AmqpFlowBean();
        val.setHandle(new AmqpHandle.AmqpHandleBean(1L));
        AmqpFlow read = marshalUnmarshal(val);
        AmqpHandle handle = read.getHandle();
        
        AmqpSequenceNo seq = read.getLimit();
        AmqpOptions options = read.getOptions();
        
        assertTrue(val.equivalent(read));
        System.out.println("Value: " + read.getValue());
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
