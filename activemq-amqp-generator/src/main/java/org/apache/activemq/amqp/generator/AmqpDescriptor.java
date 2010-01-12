package org.apache.activemq.amqp.generator;

import org.apache.activemq.amqp.generator.jaxb.schema.Descriptor;

public class AmqpDescriptor {

    String code;
    String name;

    public void parseFromDescriptor(Descriptor descriptor) {

        code = descriptor.getCode();
        name = descriptor.getName();
        // TODO descriptor.getDoc();
    }

    public String toString() {
        return "{" + name + ", code=" + code + "}";
    }

}
