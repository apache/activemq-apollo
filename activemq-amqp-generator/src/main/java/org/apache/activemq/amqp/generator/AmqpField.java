package org.apache.activemq.amqp.generator;

import static org.apache.activemq.amqp.generator.Utils.toJavaName;

import org.apache.activemq.amqp.generator.jaxb.schema.Doc;
import org.apache.activemq.amqp.generator.jaxb.schema.Field;

public class AmqpField {

    AmqpDoc doc;
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
            if (object instanceof Doc) {
                if (doc == null) {
                    doc = new AmqpDoc();
                }
                doc.parseFromDoc((Doc) object);
            } else {
                // TODO handle exception:
            }
        }
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public AmqpDoc getDoc() {
        return doc;
    }

    public void setDoc(AmqpDoc doc) {
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

    public AmqpClass resolveAmqpFieldType() throws UnknownTypeException {
        if (isMultiple()) {
            return TypeRegistry.resolveAmqpClass("list");
        }

        AmqpClass ampqClass = TypeRegistry.resolveAmqpClass(this);
        return ampqClass;
    }

    public String getJavaName() {
        return toJavaName(name);
    }

    public String toString() {
        return name + "[type:" + type + ", req:" + required + " multiple: " + multiple + ", def:" + defaultValue + "]";
    }
}
