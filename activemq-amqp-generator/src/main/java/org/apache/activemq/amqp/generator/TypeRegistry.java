package org.apache.activemq.amqp.generator;

import java.util.Collection;
import java.util.HashMap;

public class TypeRegistry {

    private static final HashMap<String, JavaTypeMapping> JAVA_TYPE_MAP = new HashMap<String, JavaTypeMapping>();
    private static final HashMap<String, AmqpClass> GENERATED_TYPE_MAP = new HashMap<String, AmqpClass>();

    static {
        JAVA_TYPE_MAP.put("boolean", new JavaTypeMapping("boolean", "java.lang.boolean"));
        JAVA_TYPE_MAP.put("ubyte", new JavaTypeMapping("ubyte", "java.lang.short"));
        JAVA_TYPE_MAP.put("ushort", new JavaTypeMapping("ushort", "java.lang.int"));
        JAVA_TYPE_MAP.put("uint", new JavaTypeMapping("uint", "java.lang.long"));
        JAVA_TYPE_MAP.put("ulong", new JavaTypeMapping("ulong", "java.math.BigInteger"));
        JAVA_TYPE_MAP.put("byte", new JavaTypeMapping("byte", "java.lang.byte"));
        JAVA_TYPE_MAP.put("short", new JavaTypeMapping("short", "java.lang.short"));
        JAVA_TYPE_MAP.put("int", new JavaTypeMapping("int", "java.lang.int"));
        JAVA_TYPE_MAP.put("long", new JavaTypeMapping("long", "java.lang.long"));
        JAVA_TYPE_MAP.put("float", new JavaTypeMapping("float", "java.lang.float"));
        JAVA_TYPE_MAP.put("double", new JavaTypeMapping("double", "java.lang.double"));
        JAVA_TYPE_MAP.put("char", new JavaTypeMapping("char", "java.lang.int"));
        JAVA_TYPE_MAP.put("timestamp", new JavaTypeMapping("timestamp", "java.util.Date"));
        JAVA_TYPE_MAP.put("uuid", new JavaTypeMapping("uuid", "java.util.UUID"));
        JAVA_TYPE_MAP.put("binary", new JavaTypeMapping("binary", "java.lang.byte", true, null));
        JAVA_TYPE_MAP.put("string", new JavaTypeMapping("string", "java.lang.String"));
        JAVA_TYPE_MAP.put("symbol", new JavaTypeMapping("symbol", "java.lang.String"));
        JAVA_TYPE_MAP.put("list", new JavaTypeMapping("list", "java.util.List", false, "<AmqpType>"));
        JAVA_TYPE_MAP.put("map", new JavaTypeMapping("map", "java.util.HashMap", false, "<AmqpType, AmqpType>"));
        JAVA_TYPE_MAP.put("null", new JavaTypeMapping("null", "java.lang.Object"));

    }

    static final void init(Generator generator) {
        // Add in the wildcard type:
        AmqpClass any = new AmqpType("*", generator.getPackagePrefix() + ".types.AmqpType");
        GENERATED_TYPE_MAP.put("*", any);
        JAVA_TYPE_MAP.put("*", any.typeMapping);
    }

    public static JavaTypeMapping getJavaTypeMapping(String name) throws UnknownTypeException {
        JavaTypeMapping mapping = JAVA_TYPE_MAP.get(name);
        if (mapping == null) {
            // Try to find a class that defines it:
            AmqpClass amqpClass = GENERATED_TYPE_MAP.get(name);
            if (amqpClass != null) {
                mapping = amqpClass.typeMapping;
            }
            if (mapping == null) {
                throw new UnknownTypeException(name);
            }
        }
        return mapping;
    }

    public static AmqpClass resolveAmqpClass(AmqpField amqpField) throws UnknownTypeException {
        return resolveAmqpClass(amqpField.getType());
    }

    public static AmqpClass resolveAmqpClass(String type) throws UnknownTypeException {
        AmqpClass amqpClass = GENERATED_TYPE_MAP.get(type);
        if (amqpClass == null) {
            throw new UnknownTypeException("Type " + type + " not found");
        }
        return amqpClass;
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

    public static Collection<AmqpClass> getGeneratedTypes() {
        return GENERATED_TYPE_MAP.values();
    }

    public static void addType(AmqpClass amqpClass) {
        GENERATED_TYPE_MAP.put(amqpClass.getName(), amqpClass);
    }

    public static class JavaTypeMapping {

        String amqpType;
        String shortName;
        String packageName;
        String fullName;
        String javaType;

        boolean array;
        String generic;

        JavaTypeMapping(String amqpType, String fullName, boolean array, String generic) {
            this(amqpType, fullName);
            this.array = array;
            this.generic = generic;
            if (generic != null) {
                javaType = javaType + generic;
            }
            if (array) {
                javaType = javaType + " []";
            }
        }

        JavaTypeMapping(String amqpType, String fullName) {
            this.amqpType = amqpType;
            this.fullName = fullName;
            this.packageName = fullName.substring(0, fullName.lastIndexOf("."));
            this.shortName = fullName.substring(fullName.lastIndexOf(".") + 1);
            this.javaType = shortName;
        }

        public String getAmqpType() {
            return amqpType;
        }

        public void setAmqpType(String amqpType) {
            this.amqpType = amqpType;
        }

        public String getShortName() {
            return shortName;
        }

        public void setShortName(String shortName) {
            this.shortName = shortName;
        }

        public String getPackageName() {
            return packageName;
        }

        public void setPackageName(String packageName) {
            this.packageName = packageName;
        }

        public String getFullName() {
            return fullName;
        }

        public void setFullName(String fullName) {
            this.fullName = fullName;
        }

        public String getJavaType() {
            return javaType;
        }

        public void setJavaType(String javaType) {
            this.javaType = javaType;
        }

        public boolean isArray() {
            return array;
        }

        public void setArray(boolean array) {
            this.array = array;
        }

        public String getGeneric() {
            return generic;
        }

        public void setGeneric(String generic) {
            this.generic = generic;
        }

        public String getImport() {
            if (packageName.startsWith("java.lang")) {
                return null;
            } else {
                return fullName;
            }
        }
    }

    public static class AmqpType extends AmqpClass {

        AmqpType(String amqpName, String fullName) {
            super.typeMapping = new JavaTypeMapping(amqpName, fullName);
            super.name = amqpName;
            super.setPrimitive(true);
            super.handcoded = true;
        }
    }

}
