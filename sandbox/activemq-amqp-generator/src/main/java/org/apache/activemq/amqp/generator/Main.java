package org.apache.activemq.amqp.generator;


public class Main {

    public static final void main(String[] args) {
        Generator gen = new Generator();
        gen.setInputFiles("C:/dev/fuse/amq6.0/activemq-amqp-generator/specification/1.0-PR2/transport.xml", "C:/dev/fuse/amq6.0/activemq-amqp-generator/specification/1.0-PR2/messaging.xml",
                "C:/dev/fuse/amq6.0/activemq-amqp-generator/specification/1.0-PR2/types.xml", "C:/dev/fuse/amq6.0/activemq-amqp-generator/specification/1.0-PR2/security.xml");
        
        gen.setOutputDirectory("C:/dev/fuse/amq6.0/activemq-amqp/src/main/java");
        gen.setSourceDirectory("C:/dev/fuse/amq6.0/activemq-amqp-generator/src/handcoded");
        try {
            gen.generate();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
