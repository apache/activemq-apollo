package org.apache.activemq.amqp.generator;

import java.io.BufferedWriter;
import java.io.IOException;

import org.apache.activemq.amqp.generator.jaxb.schema.Definition;

public class AmqpDefinition {

    Definition definition;
    AmqpDoc doc;

    AmqpDefinition(Definition definition) {
        parseFromDefinition(definition);
    }

    public void parseFromDefinition(Definition definition) {
        this.definition = definition;
        if (definition.getDoc() != null || definition.getDoc() != null) {
            doc = new AmqpDoc(definition.getDoc());
            doc.setLabel(definition.getLabel());
        }
    }

    public String getLabel() {
        return definition.getLabel();
    }

    public void writeJavaDoc(BufferedWriter writer, int indent) throws IOException {
        doc.writeJavaDoc(writer, indent);
    }

    public String getValue() {
        return definition.getValue();
    }

    public String getName() {
        return definition.getName();
    }

}
