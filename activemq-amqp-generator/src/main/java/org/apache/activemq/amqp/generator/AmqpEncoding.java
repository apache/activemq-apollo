package org.apache.activemq.amqp.generator;

import org.apache.activemq.amqp.generator.jaxb.schema.Encoding;

public class AmqpEncoding {

    private String name;
    private String category;
    private String code;
    private String width;

    public void parseFromEncoding(Encoding encoding) {
        name = encoding.getName();
        category = encoding.getCategory();
        code = encoding.getCode();
        width = encoding.getWidth();
        //TODO: encoding.getDoc();
    }

    public String toString() {
        return "{" + name + ", cat=" + category + ", code=" + code + ", width=" + width +"}";
    }
}
