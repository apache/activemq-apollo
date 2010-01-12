package org.apache.activemq.amqp.generator;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;
import static org.apache.activemq.amqp.generator.Utils.*;
import org.apache.activemq.amqp.generator.jaxb.schema.*;

public class AmqpClass {

    protected String name;
    protected AmqpDoc doc;
    protected AmqpChoice choice;
    protected AmqpException exception;
    protected AmqpDescriptor descriptor;
    protected AmqpEncoding encoding;
    protected String restrictedType;

    protected boolean restricted;
    protected boolean primitive;

    LinkedHashMap<String, AmqpField> fields = new LinkedHashMap<String, AmqpField>();
    protected String javaPackage = "";
    protected String javaType = "";

    public void parseFromType(Generator generator, String source, Type type) {
        this.name = type.getName();
        this.restrictedType = type.getSource();

        // See if this is a restricting type (used to restrict the type of a
        // field):
        if (type.getClazz().equalsIgnoreCase("restricted")) {
            this.restricted = true;
        }

        for (Object typeAttribute : type.getEncodingOrDescriptorOrFieldOrChoiceOrExceptionOrDoc()) {
            if (typeAttribute instanceof Field) {
                AmqpField field = new AmqpField();
                field.parseFromField((Field) typeAttribute);
                fields.put(field.getName(), field);
            } else if (typeAttribute instanceof Descriptor) {
                descriptor = new AmqpDescriptor();
                descriptor.parseFromDescriptor((Descriptor) typeAttribute);
            } else if (typeAttribute instanceof Choice) {
                if (choice == null) {
                    choice = new AmqpChoice();
                }
                choice.parseFromChoice((Choice) typeAttribute);
            } else if (typeAttribute instanceof Doc) {
                doc = new AmqpDoc();
                doc.parseFromDoc((Doc) typeAttribute);
            } else if (typeAttribute instanceof Encoding) {
                encoding = new AmqpEncoding();
                encoding.parseFromEncoding((Encoding) typeAttribute);
            } else if (typeAttribute instanceof org.apache.activemq.amqp.generator.jaxb.schema.Exception) {
                exception = new AmqpException();
                exception.parseFromException((org.apache.activemq.amqp.generator.jaxb.schema.Exception) typeAttribute);
            }
        }

        if (type.getClazz().equalsIgnoreCase("primitive")) {
            javaType = "Amqp" + capFirst(toJavaName(name));
            setPrimitive(true);
        } else {
            javaType = capFirst(toJavaName(name));
        }
        javaPackage = generator.getPackagePrefix() + "." + source;
    }

    public void generate(Generator generator) throws IOException, UnknownTypeException {
        String className = getJavaType();

        File file = new File(generator.getOutputDirectory() + File.separator + new String(javaPackage + "." + className).replace(".", File.separator) + ".java");
        file.getParentFile().mkdirs();
        if (file.exists()) {
            file.delete();
        }
        BufferedWriter writer = new BufferedWriter(new FileWriter(file));

        writeCopyWrite(writer);
        writer.write("package " + javaPackage + ";\n");
        writer.newLine();
        if (writeImports(writer)) {
            writer.newLine();
        }

        writer.write("public class " + className + " {");
        writer.newLine();
        writer.newLine();

        if (writeConstants(writer)) {
            writer.newLine();
            writer.newLine();
        }

        if (writeFields(writer)) {
            writer.newLine();
        }

        if (writeFieldAccesors(writer)) {
            writer.newLine();
        }
        writer.write("}");
        writer.flush();
        writer.close();

    }

    private static void writeCopyWrite(BufferedWriter writer) throws IOException {
        writer.write("/**");
        writer.newLine();
        writer.write(" * Licensed to the Apache Software Foundation (ASF) under one or more");
        writer.newLine();
        writer.write(" * contributor license agreements.  See the NOTICE file distributed with");
        writer.newLine();
        writer.write(" * this work for additional information regarding copyright ownership.");
        writer.newLine();
        writer.write(" * The ASF licenses this file to You under the Apache License, Version 2.0");
        writer.newLine();
        writer.write(" * (the \"License\"); you may not use this file except in compliance with");
        writer.newLine();
        writer.write(" * the License.  You may obtain a copy of the License at");
        writer.newLine();
        writer.write(" *");
        writer.newLine();
        writer.write(" *      http://www.apache.org/licenses/LICENSE-2.0");
        writer.newLine();
        writer.write(" *");
        writer.newLine();
        writer.write(" * Unless required by applicable law or agreed to in writing, software");
        writer.newLine();
        writer.write(" * distributed under the License is distributed on an \"AS IS\" BASIS,");
        writer.newLine();
        writer.write(" * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.");
        writer.newLine();
        writer.write(" * See the License for the specific language governing permissions and");
        writer.newLine();
        writer.write(" * limitations under the License.");
        writer.newLine();
        writer.write(" */");
        writer.newLine();
        writer.newLine();
    }

    private boolean writeImports(BufferedWriter writer) throws IOException, UnknownTypeException {

        HashSet<String> imports = new HashSet<String>();

        for (AmqpField field : fields.values()) {
            imports.add(field.getJavaPackage());
        }

        imports.remove(getJavaPackage());
        imports.remove("java.lang");

        boolean ret = false;

        for (String toImport : imports) {
            ret = true;
            writer.write("import " + toImport + ".*;");
            writer.newLine();
        }
        return ret;
    }

    private boolean writeConstants(BufferedWriter writer) throws IOException, UnknownTypeException {
        boolean ret = false;
        if (choice != null) {
            ret = true;
            for (Map.Entry<String, String> constant : choice.choices.entrySet()) {
                writer.write(tab(1) + "public static final " + TypeRegistry.getJavaType(restrictedType) + " " + Utils.toJavaConstant(constant.getKey() + " = " + constant.getValue() + ";"));
                writer.newLine();
            }
        }
        return ret;
    }

    private boolean writeFields(BufferedWriter writer) throws IOException, UnknownTypeException {
        boolean ret = false;
        for (AmqpField field : fields.values()) {
            ret = true;
            if (field.getDoc() != null) {
                // TODO
            }

            String array = field.isMultiple() ? " []" : "";
            writer.write(tab(1) + "private " + field.getJavaType() + array + " " + field.getJavaName());

            if (field.getDefaultValue() != null) {
                writer.write(" = " + field.getDefaultValue());
            }

            writer.write(";");
            writer.newLine();
        }
        return ret;
    }

    private boolean writeFieldAccesors(BufferedWriter writer) throws IOException, UnknownTypeException {
        boolean ret = false;
        for (AmqpField field : fields.values()) {
            ret = true;
            if (field.getDoc() != null) {
                // TODO
            }

            String array = field.isMultiple() ? " []" : "";

            // Setter:
            writer.write(tab(1) + "public final void set" + capFirst(field.getJavaName()) + "(" + field.getJavaType() + array + " " + toJavaName(field.getName()) + ") {");
            writer.newLine();
            writer.write(tab(2) + "this." + field.getJavaName() + " = " + field.getJavaName() + ";");
            writer.newLine();
            writer.write(tab(1) + "}");
            writer.newLine();
            writer.newLine();
            // Getter:

            // Setter:
            writer.write(tab(1) + "public final " + field.getJavaType() + array + " get" + capFirst(field.getJavaName()) + "() {");
            writer.newLine();
            writer.write(tab(2) + "return " + field.getJavaName() + ";");
            writer.newLine();
            writer.write(tab(1) + "}");
            writer.newLine();
            writer.newLine();
        }
        return ret;
    }

    public String getJavaType() {
        return javaType;
    }

    public String getJavaPackage() {
        return javaPackage;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isPrimitive() {
        return primitive;
    }

    public boolean isRestricted() {
        return restricted;
    }

    public String getRestrictedType() {
        return restrictedType;
    }

    public void setPrimitive(boolean primitive) {
        this.primitive = primitive;
    }

    public String toString() {
        String ret = "Class: " + name + " [Encoding=" + encoding + ", Descriptor=" + descriptor + "]\n";
        ret += " Fields:\n";
        for (AmqpField f : fields.values()) {
            ret += " " + f.toString();
        }

        return ret;
    }

}
