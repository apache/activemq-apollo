package org.apache.activemq.amqp.generator;

import java.util.StringTokenizer;

import org.apache.activemq.amqp.generator.jaxb.schema.Descriptor;

public class AmqpDescriptor {

    String formatCode;
    String symbolicName;
    long category;
    long descriptorId;

    public void parseFromDescriptor(Descriptor descriptor) {

        formatCode = descriptor.getCode();
        symbolicName = descriptor.getName();
        StringTokenizer tok = new StringTokenizer(formatCode, ":");
        category = Long.parseLong(tok.nextToken().substring(2), 16);
        descriptorId = Long.parseLong(tok.nextToken().substring(2), 16);
        // TODO descriptor.getDoc();
    }

    public String getFormatCode() {
        return formatCode;
    }

    public void setFormatCode(String formatCode) {
        this.formatCode = formatCode;
    }

    public String getSymbolicName() {
        return symbolicName;
    }

    public void setSymbolicName(String symbolicName) {
        this.symbolicName = symbolicName;
    }

    public long getCategory() {
        return category;
    }

    public void setCategory(long category) {
        this.category = category;
    }

    public long getDescriptorId() {
        return descriptorId;
    }

    public void setDescriptorId(long descriptorId) {
        this.descriptorId = descriptorId;
    }

    public String toString() {
        return "{" + symbolicName + ", code=" + formatCode + "}";
    }

}
