package org.apache.activemq.amqp.generator.handcoded;

import java.io.IOException;

public class AmqpFramingException extends IOException{

    private static final long serialVersionUID = 1L;

    public AmqpFramingException(String msg)
    {
        super(msg);
    }
    
    public AmqpFramingException(String msg, Throwable cause)
    {
        super(msg, cause);
    }
}
