package org.apache.activemq.amqp.generator;

import java.util.Collection;
import java.util.HashMap;

public class TypeRegistry {

    private static final HashMap<String, JavaTypeMapping> JAVA_TYPE_MAP = new HashMap<String, JavaTypeMapping>();
    private static final HashMap<String, AmqpClass> GENERATED_TYPE_MAP = new HashMap<String, AmqpClass>();

    static {
        JAVA_TYPE_MAP.put("boolean", new JavaTypeMapping("boolean", "boolean", "java.lang"));
        JAVA_TYPE_MAP.put("ubyte", new JavaTypeMapping("ubyte", "short", "java.lang"));
        JAVA_TYPE_MAP.put("ushort", new JavaTypeMapping("ushort", "int", "java.lang"));
        JAVA_TYPE_MAP.put("uint", new JavaTypeMapping("uint", "long", "java.lang"));
        JAVA_TYPE_MAP.put("ulong", new JavaTypeMapping("ulong", "BigInteger", "java.math"));
        JAVA_TYPE_MAP.put("byte", new JavaTypeMapping("byte", "byte", "java.lang"));
        JAVA_TYPE_MAP.put("short", new JavaTypeMapping("short", "short", "java.lang"));
        JAVA_TYPE_MAP.put("int", new JavaTypeMapping("int", "int", "java.lang"));
        JAVA_TYPE_MAP.put("long", new JavaTypeMapping("long", "long", "java.lang"));
        JAVA_TYPE_MAP.put("float", new JavaTypeMapping("float", "float", "java.lang"));
        JAVA_TYPE_MAP.put("double", new JavaTypeMapping("double", "double", "java.lang"));
        JAVA_TYPE_MAP.put("char", new JavaTypeMapping("char", "char", "java.lang"));
        JAVA_TYPE_MAP.put("timestamp", new JavaTypeMapping("timestamp", "Date", "java.util"));
        JAVA_TYPE_MAP.put("uuid", new JavaTypeMapping("uuid", "UUID", "java.util"));
        JAVA_TYPE_MAP.put("byte", new JavaTypeMapping("byte", "byte", "java.lang"));
        JAVA_TYPE_MAP.put("binary", new JavaTypeMapping("binary", "byte []", "java.lang"));
        JAVA_TYPE_MAP.put("string", new JavaTypeMapping("string", "String", "java.lang"));
        JAVA_TYPE_MAP.put("symbol", new JavaTypeMapping("symbol", "String", "java.lang"));
        JAVA_TYPE_MAP.put("list", new JavaTypeMapping("list", "List <AmqpType>", "java.util"));
        JAVA_TYPE_MAP.put("map", new JavaTypeMapping("map", "HashMap <AmqpType, AmqpType>", "java.util"));
        JAVA_TYPE_MAP.put("null", new JavaTypeMapping("null", "Object", "java.lang"));

    }

    static final void init(Generator generator) {
        // Add in the wildcard type:
        GENERATED_TYPE_MAP.put("*", new AnyClass("*", "AmqpAny", generator.getPackagePrefix() + ".types"));
        JAVA_TYPE_MAP.put("*", new JavaTypeMapping("*", "byte []", "java.lang"));
        
        // Add in the compound type:
    }

    public static String getJavaType(AmqpField field) throws UnknownTypeException {
        return getJavaType(field.getType());
    }

    public static String getJavaType(String type) throws UnknownTypeException {
        AmqpClass amqpClass = GENERATED_TYPE_MAP.get(type);
        if (amqpClass == null) {
            throw new UnknownTypeException("Type " + type + " not found");
        }

        // Replace with restricted type:
        if (amqpClass.isRestricted()) {
            return getJavaType(amqpClass.getRestrictedType());
        }

        if (amqpClass.isPrimitive()) {
            JavaTypeMapping mapping = JAVA_TYPE_MAP.get(amqpClass.getName());
            if (mapping == null) {
                throw new UnknownTypeException("Primitive Type " + type + " not found");
            }
            return mapping.javaType;
        }

        return amqpClass.getJavaType();
    }

    public static String getJavaPackage(AmqpField field) throws UnknownTypeException {
        return getJavaPackage(field.getType());
    }

    public static String getJavaPackage(String type) throws UnknownTypeException {
        AmqpClass amqpClass = GENERATED_TYPE_MAP.get(type);
        if (amqpClass == null) {
            throw new UnknownTypeException("Type " + type + " not found");
        }

        // Replace with restricted type:
        if (amqpClass.isRestricted()) {
            return getJavaPackage(amqpClass.getRestrictedType());
        }

        if (amqpClass.isPrimitive()) {
            JavaTypeMapping mapping = JAVA_TYPE_MAP.get(amqpClass.getName());
            if (mapping == null) {
                throw new UnknownTypeException("Primitive Type " + type + " not found");
            }
            return mapping.javaPackage;
        }
        return amqpClass.getJavaPackage();
    }

    public static Collection<AmqpClass> getGeneratedTypes() {
        return GENERATED_TYPE_MAP.values();
    }

    public static void addType(AmqpClass amqpClass) {
        GENERATED_TYPE_MAP.put(amqpClass.getName(), amqpClass);
    }

    public static class JavaTypeMapping {

        String name;
        String javaType;
        String javaPackage;

        JavaTypeMapping(String name, String javaType, String javaPackage) {
            this.name = name;
            this.javaType = javaType;
            this.javaPackage = javaPackage;
        }
    }

    public static class AnyClass extends AmqpClass {

        AnyClass(String name, String javaType, String javaPackage) {
            super.name = name;
            super.javaType = javaType;
            super.setPrimitive(true);
            super.javaPackage = javaPackage;
            super.handcoded = true;
        }
    }
    
    public static class CompoundType extends AmqpClass {

        CompoundType(String name, String javaType, String javaPackage) {
            super.name = name;
            super.javaType = javaType;
            super.setPrimitive(true);
            super.javaPackage = javaPackage;
        }
    }
    
    
}
