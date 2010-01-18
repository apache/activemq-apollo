package org.apache.activemq.amqp.generator;

import java.util.LinkedList;

import org.apache.activemq.amqp.generator.jaxb.schema.Choice;

public class AmqpChoice {

    LinkedList<Choice> choices = new LinkedList<Choice>();
    public void parseFromChoice(Choice choice) {
        choices.add(choice);
    }
}
