package org.apache.activemq.amqp.generator;

import java.util.LinkedHashMap;

import org.apache.activemq.amqp.generator.jaxb.schema.Choice;

public class AmqpChoice {

    private AmqpDoc doc;
    private String name;
    
    LinkedHashMap<String, String> choices = new LinkedHashMap<String, String>();
    public void parseFromChoice(Choice choice) {
        choices.put(choice.getName(), choice.getValue());
        //TODO choice.getDoc();
    }
}
