package org.apache.activemq.amqp.generator;

import org.apache.activemq.amqp.generator.jaxb.schema.Exception;

public class AmqpException {

    String errorCode; 
    String name;
    
    public void parseFromException(Exception exception)
    {
        errorCode = exception.getErrorCode();
        name = exception.getName();
        //TODO exception.getDoc()
    }
}
