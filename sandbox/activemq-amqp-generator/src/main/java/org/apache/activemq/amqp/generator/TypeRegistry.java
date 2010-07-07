package org.apache.activemq.amqp.generator;

import java.util.Collection;
import java.util.HashMap;

public class TypeRegistry {

    private static final HashMap<String, JavaTypeMapping> JAVA_TYPE_MAP = new HashMap<String, JavaTypeMapping>();
    private static final HashMap<String, AmqpClass> GENERATED_TYPE_MAP = new HashMap<String, AmqpClass>();

    static final void init(Generator generator) {

        // Add in the wildcard type:
        AmqpClass any = new AmqpType("*", generator.getPackagePrefix() + ".types.AmqpType");
        GENERATED_TYPE_MAP.put("*", any);
        JAVA_TYPE_MAP.put("*", any.typeMapping);

        //
        JAVA_TYPE_MAP.put("boolean", new JavaTypeMapping("boolean", "java.lang", "Boolean", "boolean"));
        JAVA_TYPE_MAP.put("ubyte", new JavaTypeMapping("ubyte", "java.lang", "Short", "short"));
        JAVA_TYPE_MAP.put("ushort", new JavaTypeMapping("ushort", "java.lang", "Integer", "int"));
        JAVA_TYPE_MAP.put("uint", new JavaTypeMapping("uint", "java.lang", "Long", "long"));
        JAVA_TYPE_MAP.put("ulong", new JavaTypeMapping("ulong", "java.math", "BigInteger"));
        JAVA_TYPE_MAP.put("byte", new JavaTypeMapping("byte", "java.lang", "Byte", "byte"));
        JAVA_TYPE_MAP.put("short", new JavaTypeMapping("short", "java.lang", "Short", "short"));
        JAVA_TYPE_MAP.put("int", new JavaTypeMapping("int", "java.lang", "Integer", "int"));
        JAVA_TYPE_MAP.put("long", new JavaTypeMapping("long", "java.lang", "Long", "long"));
        JAVA_TYPE_MAP.put("float", new JavaTypeMapping("float", "java.lang", "Float", "float"));
        JAVA_TYPE_MAP.put("double", new JavaTypeMapping("double", "java.lang", "Double", "double"));
        JAVA_TYPE_MAP.put("char", new JavaTypeMapping("char", "java.lang", "Integer", "int"));
        JAVA_TYPE_MAP.put("timestamp", new JavaTypeMapping("timestamp", "java.util", "Date"));
        JAVA_TYPE_MAP.put("uuid", new JavaTypeMapping("uuid", "java.util", "UUID"));
        JAVA_TYPE_MAP.put("binary", new JavaTypeMapping("binary", "org.apache.activemq.util.buffer", "Buffer"));
        JAVA_TYPE_MAP.put("string", new JavaTypeMapping("string", "java.lang", "String"));
        JAVA_TYPE_MAP.put("symbol", new JavaTypeMapping("symbol", "java.lang", "String"));
        JavaTypeMapping list = new JavaTypeMapping("list", generator.getPackagePrefix() + ".types", "IAmqpList");
        list.setGenerics("E extends " + any.getJavaType());
        JAVA_TYPE_MAP.put("list", list);
        JavaTypeMapping map = new JavaTypeMapping("map", generator.getPackagePrefix() + ".types", "IAmqpMap");
        map.setGenerics("K extends " + any.getJavaType(), "V extends " + any.getJavaType());
        JAVA_TYPE_MAP.put("map", map);
        JAVA_TYPE_MAP.put("null", new JavaTypeMapping("null", "java.lang", "Object"));

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

    public static AmqpClass any() {
        return GENERATED_TYPE_MAP.get("*");
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
            return mapping.className;
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

        private String amqpType;
        private String packageName;
        private String fullName;
        private String className;
        private String primitiveType;
        private String javaType;
        private boolean array;
        private String[] generics;

        JavaTypeMapping(JavaTypeMapping other) {
            this.amqpType = other.amqpType;
            this.packageName = other.packageName;
            this.fullName = other.fullName;
            this.className = other.className;
            this.primitiveType = other.primitiveType;
            this.array = other.array;
            this.javaType = other.javaType;
            if (other.generics != null) {
                this.generics = new String[other.generics.length];
                System.arraycopy(other.generics, 0, generics, 0, generics.length);
            }
        }

        public JavaTypeMapping(String amqpType, String fullName) {
            this(amqpType, fullName.substring(0, fullName.lastIndexOf(".")), fullName.substring(fullName.lastIndexOf(".") + 1), null);
        }

        public JavaTypeMapping(String amqpType, String packageName, String className) {
            this(amqpType, packageName, className, null);
        }

        JavaTypeMapping(String amqpType, String packageName, String className, String primitiveType) {
            this.amqpType = amqpType;
            this.fullName = packageName + "." + className;
            this.packageName = packageName;
            this.className = className;
            this.javaType = className;
            this.primitiveType = primitiveType;
        }

        public boolean hasPrimitiveType() {
            return primitiveType != null;
        }

        public String getPrimitiveType() {
            if (primitiveType == null) {
                return className;
            }
            return primitiveType;
        }

        public String getAmqpType() {
            return amqpType;
        }

        public void setAmqpType(String amqpType) {
            this.amqpType = amqpType;
        }

        public String getClassName() {
            return className;
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
            return className;
        }

        public JavaTypeMapping shortName() {
            if (javaType.indexOf(".") > 0) {
                JavaTypeMapping rc = new JavaTypeMapping(this);
                rc.javaType = javaType.substring(javaType.indexOf(".") + 1);
                return rc;
            } else {
                return this;
            }
        }

        public JavaTypeMapping nonGeneric() {
            if (generics != null) {
                JavaTypeMapping rc = new JavaTypeMapping(this);
                rc.javaType = javaType.substring(javaType.indexOf(".") + 1);
                rc.generics = null;
                return rc;
            } else {
                return this;
            }
        }

        public JavaTypeMapping parameterized() {
            if (generics != null) {
                JavaTypeMapping rc = new JavaTypeMapping(this);
                for (int i = 0; i < generics.length; i++) {
                    if (rc.generics[i].indexOf("extends") > 0) {
                        rc.generics[i] = rc.generics[i].substring(0, rc.generics[i].indexOf("extends")).trim();
                    }
                }
                return rc;
            } else {
                return this;
            }
        }
        
        public JavaTypeMapping wildcard() {
            if (generics != null) {
                JavaTypeMapping rc = new JavaTypeMapping(this);
                for (int i = 0; i < generics.length; i++) {
                    rc.generics[i] = "?";
                }
                return rc;
            } else {
                return this;
            }
        }

        public JavaTypeMapping parameterized(String... generics) {
            JavaTypeMapping rc = new JavaTypeMapping(this);
            rc.generics = generics;
            return rc;
        }

        public boolean isArray() {
            return array;
        }

        public void setArray(boolean array) {
            this.array = array;
        }

        public boolean isGeneric() {
            return generics != null;
        }

        public String[] getGenerics() {
            return generics;
        }
        
        public void setGenerics(String... generics) {
            this.generics = generics;
        }

        public String getFullVersionMarshallerName(Generator generator) {
            return generator.getMarshallerPackage() + "." + className + "Marshaller";
        }

        public String getImport() {
            return fullName;
        }

        public String toString() {
            return javaType + getGeneric("");
        }

        public String getGeneric(String suffix) {
            if (generics != null && generics.length > 0) {
                String rc = "<";
                boolean first = true;
                for (String g : generics) {
                    if (!first) {
                        rc += ", ";
                    } else {
                        first = false;
                    }
                    rc += g;
                }
                return rc + ">" + (suffix != null ? suffix : "");
            }
            return "";
        }
    }

    public static class AmqpType extends AmqpClass {

        AmqpType(String amqpName, String fullName) {
            super.typeMapping = new JavaTypeMapping(amqpName, fullName);
            typeMapping.setGenerics("?", "?");
            super.name = amqpName;
            super.setPrimitive(true);
            super.handcoded = true;
            super.valueMapping = typeMapping;
        }
    }

}
