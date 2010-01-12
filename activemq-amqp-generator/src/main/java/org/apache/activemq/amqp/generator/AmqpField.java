package org.apache.activemq.amqp.generator;

import org.apache.activemq.amqp.generator.jaxb.schema.*;
import static org.apache.activemq.amqp.generator.Utils.*;

public class AmqpField {

    String doc;
    String name;
    String defaultValue;
    String label;
    String type;
    boolean multiple;
    boolean required;

    public void parseFromField(Field field) {
        defaultValue = field.getDefault();
        label = field.getLabel();
        name = field.getName();
        multiple = new Boolean(field.getMultiple()).booleanValue();
        required = new Boolean(field.getRequired()).booleanValue();
        type = field.getType();

        for (Object object : field.getDocOrException()) {
            // TODO;
        }
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDoc() {
        return doc;
    }

    public void setDoc(String doc) {
        this.doc = doc;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public boolean isMultiple() {
        return multiple;
    }

    public void setMultiple(boolean multiple) {
        this.multiple = multiple;
    }

    public boolean isRequired() {
        return required;
    }

    public void setRequired(boolean required) {
        this.required = required;
    }

    public String getJavaType() throws UnknownTypeException {
        return TypeRegistry.getJavaType(this);
    }
    
    public String getJavaPackage() throws UnknownTypeException {
        return TypeRegistry.getJavaPackage(this);
    }
    
    public String getJavaName() {
        return toJavaName(name);
    }

    public String toString() {
        return name + "[type:" + type + ", req:" + required + " multiple: " + multiple + ", def:" + defaultValue + "]";
    }
}
