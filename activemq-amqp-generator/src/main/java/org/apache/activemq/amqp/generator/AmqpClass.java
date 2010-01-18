package org.apache.activemq.amqp.generator;

import static org.apache.activemq.amqp.generator.Utils.capFirst;
import static org.apache.activemq.amqp.generator.Utils.padHex;
import static org.apache.activemq.amqp.generator.Utils.tab;
import static org.apache.activemq.amqp.generator.Utils.toJavaConstant;
import static org.apache.activemq.amqp.generator.Utils.toJavaName;
import static org.apache.activemq.amqp.generator.Utils.writeJavaCopyWrite;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.TreeSet;

import org.apache.activemq.amqp.generator.TypeRegistry.JavaTypeMapping;
import org.apache.activemq.amqp.generator.jaxb.schema.Choice;
import org.apache.activemq.amqp.generator.jaxb.schema.Descriptor;
import org.apache.activemq.amqp.generator.jaxb.schema.Doc;
import org.apache.activemq.amqp.generator.jaxb.schema.Encoding;
import org.apache.activemq.amqp.generator.jaxb.schema.Field;
import org.apache.activemq.amqp.generator.jaxb.schema.Type;

public class AmqpClass {

    protected String name;
    protected String label;
    protected AmqpDoc doc;
    protected AmqpChoice choice;
    protected AmqpException exception;
    protected AmqpDescriptor descriptor;
    protected LinkedList<AmqpEncoding> encodings;
    protected String restrictedType;

    protected boolean restricted;
    protected boolean primitive;

    LinkedHashMap<String, AmqpField> fields = new LinkedHashMap<String, AmqpField>();
    public boolean handcoded;

    // Java mapping for this class:
    public TypeRegistry.JavaTypeMapping typeMapping;
    // Java mapping of the value that this type holds (if any)
    public TypeRegistry.JavaTypeMapping valueMapping;

    public void parseFromType(Generator generator, String source, Type type) throws UnknownTypeException {
        this.name = type.getName();
        this.restrictedType = type.getSource();
        this.label = type.getLabel();

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
                if (encodings == null) {
                    encodings = new LinkedList<AmqpEncoding>();
                }
                AmqpEncoding encoding = new AmqpEncoding();
                encoding.parseFromEncoding((Encoding) typeAttribute);
                encodings.add(encoding);
            } else if (typeAttribute instanceof org.apache.activemq.amqp.generator.jaxb.schema.Exception) {
                exception = new AmqpException();
                exception.parseFromException((org.apache.activemq.amqp.generator.jaxb.schema.Exception) typeAttribute);
            }
        }

        if (type.getClazz().equalsIgnoreCase("primitive")) {
            setPrimitive(true);
        }

        // See if this is a restricting type (used to restrict the type of a
        // field):
        if (type.getClazz().equalsIgnoreCase("restricted")) {
            this.restricted = true;
        }

        typeMapping = new JavaTypeMapping(name, generator.getPackagePrefix() + "." + source + "." + "Amqp" + capFirst(toJavaName(name)));
    }

    public void generate(Generator generator) throws IOException, UnknownTypeException {
        if (handcoded) {
            return;
        }
        String className = getJavaType();

        File file = new File(generator.getOutputDirectory() + File.separator + new String(typeMapping.getFullName()).replace(".", File.separator) + ".java");
        file.getParentFile().mkdirs();
        if (file.exists()) {
            file.delete();
        }
        BufferedWriter writer = new BufferedWriter(new FileWriter(file));

        writeJavaCopyWrite(writer);
        writer.write("package " + typeMapping.getPackageName() + ";\n");
        writer.newLine();
        if (writeImports(writer, generator)) {
            writer.newLine();
        }

        if (doc != null) {
            doc.writeJavaDoc(writer, 0);
        } else if (label != null) {
            Utils.writeJavaComment(writer, 0, "Represents a " + label);
        }

        // We use enums for restricted types with a choice:
        if (isRestricted() && choice != null) {
            writer.write("public enum " + className);
        } else {
            writer.write("public class " + className);
            if (isPrimitive() || descriptor != null) {
                writer.write(" extends AmqpType");
            } else if (isRestricted()) {
                writer.write(" extends " + resolveRestrictedType().getTypeMapping().getShortName());
            }
        }

        writer.write(" {");
        writer.newLine();

        writeConstants(writer);

        writeFields(writer);

        writeConstructors(writer);

        writeFieldAccesors(writer);

        writeSerializers(writer);

        writer.write("}");
        writer.flush();
        writer.close();

    }

    private boolean writeImports(BufferedWriter writer, Generator generator) throws IOException, UnknownTypeException {

        TreeSet<String> imports = new TreeSet<String>();
        for (AmqpField field : fields.values()) {

            AmqpClass fieldType = field.resolveAmqpFieldType();
            filterOrAddImport(imports, fieldType.getTypeMapping());
            filterOrAddImport(imports, fieldType.resolveValueMapping());
            if (fieldType.choice != null) {
                filterOrAddImport(imports, fieldType.resolveBaseTypeMapping());
            }

            if (fieldType.getName().equals("*")) {
                imports.add(generator.getPackagePrefix() + ".AmqpMarshaller");
            }
        }

        if (isPrimitive() || descriptor != null) {

            imports.add(generator.getPackagePrefix() + ".types.AmqpType");
            imports.add("java.io.DataOutputStream");
            imports.add("java.io.DataInputStream");
            imports.add("java.io.IOException");
        }

        if (isPrimitive()) {
            filterOrAddImport(imports, resolveValueMapping());
            //Need the AmqpMarshaller to help with encodings:
            if (hasNonZeroEncoding() || encodings.size() > 1) {
                imports.add(generator.getPackagePrefix() + ".AmqpMarshaller");
            }
            imports.add("java.io.UnsupportedEncodingException");
        }

        if (isRestricted()) {
            if (choice != null) {
                imports.add("java.io.UnsupportedEncodingException");
            }
            imports.add(TypeRegistry.resolveAmqpClass(restrictedType).getTypeMapping().getImport());
        }

        if (descriptor != null) {
            AmqpClass describedType = descriptor.resolveDescribedType();
            if (describedType.getName().equals("list")) {
                imports.add("java.util.ArrayList");
            } else if (describedType.getName().equals("map")) {
                // Import symbol which is used for the keys:
                imports.add(TypeRegistry.resolveAmqpClass("symbol").getTypeMapping().getImport());
            }

            filterOrAddImport(imports, describedType.getTypeMapping());
            // filterOrAddImport(imports, describedType.resolveValueMapping());
        }

        boolean ret = false;

        for (String toImport : imports) {
            ret = true;
            writer.write("import " + toImport + ";");
            writer.newLine();
        }
        return ret;
    }

    private void filterOrAddImport(TreeSet<String> imports, JavaTypeMapping mapping) {
        if (mapping == null) {
            return;
        }
        if (mapping.getImport() == null) {
            return;
        }
        if (mapping.getPackageName().equals(typeMapping.getPackageName())) {
            return;
        }
        imports.add(mapping.getImport());
    }

    private boolean writeConstants(BufferedWriter writer) throws IOException, UnknownTypeException {
        boolean ret = false;

        // Write out the descriptor (for compound types):
        if (descriptor != null) {
            ret = true;
            writer.newLine();
            writer.write(tab(1) + "public static final String SYMBOLIC_NAME = \"" + descriptor.getSymbolicName() + "\";");
            writer.newLine();
            writer.write(tab(1) + "//Format code: " + descriptor.getFormatCode() + ":");
            writer.newLine();
            writer.write(tab(1) + "public static final long CATEGORY = " + descriptor.getCategory() + ";");
            writer.newLine();
            writer.write(tab(1) + "public static final long DESCRIPTOR_ID = " + descriptor.getDescriptorId() + ";");
            writer.newLine();
            writer.write(tab(1) + "public static final long FORMAT_CODE = CATEGORY << 32 | DESCRIPTOR_ID; //(" + (descriptor.getCategory() << 32 | descriptor.getDescriptorId()) + "L)");
            writer.newLine();
            writer.write(tab(1) + "//Hard coded constructor (minus the trailing primitive format code whose encoding may vary):");
            writer.newLine();
            writer.write(tab(1) + "public static final byte [] CONSTRUCTOR = new byte [] {");
            writer.newLine();
            writer.write(tab(2) + "(byte) 0x00,                                         // COMPOUND_TYPE);");
            writer.newLine();
            // TODO retrieve ulong encoding from the ulong itself:
            writer.write(tab(2) + "(byte) 0x80,                                         // ulong descriptor encoding)");
            writer.newLine();
            // Add the category code:
            writer.write(tab(2));
            String categoryHex = padHex(descriptor.getFormatCode().substring(2, descriptor.getFormatCode().indexOf(":")), 8);
            for (int i = 0; i < 8; i += 2) {
                writer.write("(byte) 0x" + categoryHex.substring(i, i + 2) + ", ");
            }
            writer.write(" // CATEGORY CODE");
            writer.newLine();
            writer.write(tab(2));
            // Add the descriptor id code:
            String descriptorIdHex = padHex(descriptor.getFormatCode().substring(descriptor.getFormatCode().indexOf(":") + 3), 8);

            for (int i = 0; i < 8; i += 2) {
                writer.write("(byte) 0x" + descriptorIdHex.substring(i, i + 2));
                if (i < 6) {
                    writer.write(", ");
                }
            }
            writer.write("   // DESCRIPTOR ID CODE");
            writer.newLine();
            writer.write(tab(1) + "};");
            writer.newLine();

            if (descriptor.getDescribedType().equals("map")) {
                writer.newLine();
                writer.write(tab(1) + "//Accessor keys for field mapped fields:");
                writer.newLine();
                for (AmqpField field : fields.values()) {

                    writer.write(tab(1) + "private static final AmqpSymbol " + toJavaConstant(field.getName()) + "_KEY = " + " new AmqpSymbol(\"" + field.getName() + "\");");
                    writer.newLine();
                }
                writer.newLine();
            }
        }

        // Write out encodings:
        if (encodings != null) {
            writeEncodings(writer);
        }

        if (choice != null) {
            ret = true;
            writer.newLine();
            int i = 0;
            AmqpClass amqpType = TypeRegistry.resolveAmqpClass(restrictedType);
            for (Choice constant : choice.choices) {
                i++;
                if (constant.getDoc() != null) {
                    AmqpDoc docs = new AmqpDoc();
                    for (Doc doc : constant.getDoc()) {
                        docs.parseFromDoc(doc);
                    }
                    docs.writeJavaDoc(writer, 1);
                }

                writer.write(tab(1) + toJavaConstant(constant.getName()) + "((" + amqpType.resolveValueMapping().getJavaType() + ") " + constant.getValue() + ")");
                if (i < choice.choices.size()) {
                    writer.write(",");
                } else {
                    writer.write(";");
                }
                writer.newLine();
            }

            writer.newLine();
            writer.write(tab(1) + "private final " + amqpType.getTypeMapping().getJavaType() + " value = new " + amqpType.getTypeMapping().getJavaType() + "();");
            writer.newLine();

            writer.newLine();
            writer.write(tab(1) + "private " + typeMapping.getShortName() + "(" + amqpType.resolveValueMapping().getJavaType() + " value) {");
            writer.newLine();
            writer.write(tab(2) + "this.value.setValue(value);");
            writer.newLine();
            writer.write(tab(1) + "}");
            writer.newLine();

            writer.newLine();
            writer.write(tab(1) + "//TODO: Remove?");
            writer.newLine();
            writer.write(tab(1) + "public " + amqpType.getTypeMapping().getJavaType() + " getValue() {");
            writer.newLine();
            writer.write(tab(2) + "return value;");
            writer.newLine();
            writer.write(tab(1) + "}");
            writer.newLine();

            writer.newLine();
            writer.write(tab(1) + "public static final " + getTypeMapping().getJavaType() + " get(" + amqpType.resolveValueMapping().getJavaType() + " value) throws UnsupportedEncodingException{");
            if (amqpType.resolveValueMapping().getJavaType().equals("int")) {
                writer.newLine();
                writer.write(tab(2) + "switch (value) {");
                writer.newLine();
                for (Choice constant : choice.choices) {
                    writer.write(tab(2) + "case " + constant.getValue() + ": {");
                    writer.newLine();
                    writer.write(tab(3) + "return " + toJavaConstant(constant.getName()) + ";");
                    writer.newLine();
                    writer.write(tab(2) + "}");
                    writer.newLine();
                }

                writer.write(tab(2) + "default: {");
                writer.newLine();
                writer.write(tab(3) + "throw new UnsupportedEncodingException();");
                writer.newLine();
                writer.write(tab(2) + "}");
                writer.newLine();
                writer.write(tab(2) + "}");
            } else {
                writer.newLine();
                i = 0;
                for (Choice constant : choice.choices) {
                    writer.write(tab(2));
                    if (i > 0) {
                        writer.write("else ");
                    }
                    writer.write("if (value == " + constant.getValue() + ") {");
                    writer.newLine();
                    writer.write(tab(3) + "return " + toJavaConstant(constant.getName()) + ";");
                    writer.newLine();
                    writer.write(tab(2) + "}");
                    writer.newLine();
                    i++;
                }

                writer.write(tab(2) + "else {");
                writer.newLine();
                writer.write(tab(3) + "throw new UnsupportedEncodingException();");
                writer.newLine();
                writer.write(tab(2) + "}");
            }
            writer.newLine();
            writer.write(tab(1) + "}");
            writer.newLine();

            // writer.newLine();
            // writer.write(tab(1) + "public static final  " +
            // getTypeMapping().getJavaType() +
            // " createFromStream(DataInputStream dis) throws IOException {");
            // writer.newLine();
            // writer.write(tab(2) + " return get(" +
            // amqpType.getTypeMapping().getJavaType() +
            // ".createFromStream(dis)).getValue();");
            // writer.newLine();
            // writer.write(tab(1) + "}");
            // writer.newLine();

        }
        return ret;
    }

    private void writeEncodings(BufferedWriter writer) throws IOException, UnknownTypeException {
        if (encodings != null && encodings.size() == 1 && "fixed".equals(encodings.getFirst().getCategory())) {
            writer.newLine();
            writer.write(tab(1) + "public static final byte FORMAT_CODE = (byte) " + encodings.getFirst().getCode() + ";");
            writer.newLine();
            writer.write(tab(1) + "public static final FormatSubCategory FORMAT_CATEGORY  = FormatSubCategory.getCategory(FORMAT_CODE);");
            writer.newLine();
        } else {
            String encodingName = getEncodingName(false);
            writer.newLine();
            writer.write(tab(1) + "public static enum " + encodingName + " {");
            writer.newLine();

            int i = 0;
            for (AmqpEncoding encoding : encodings) {
                i++;
                String eName = encoding.getName();
                if (eName == null) {
                    eName = name;
                }
                eName = toJavaConstant(eName);

                writer.write(tab(2) + eName + " ((byte) " + encoding.getCode() + ")");
                if (i < encodings.size()) {
                    writer.write(",");
                } else {
                    writer.write(";");
                }

                if (encoding.getLabel() != null) {
                    writer.write(" // " + encoding.getLabel());
                }

                writer.newLine();
            }

            writer.newLine();
            writer.write(tab(2) + "public final byte FORMAT_CODE;");
            writer.newLine();
            writer.write(tab(2) + "public final FormatSubCategory CATEGORY;");
            writer.newLine();

            // Write constructor:
            writer.newLine();
            writer.write(tab(2) + encodingName + "(byte formatCode) {");
            writer.newLine();
            writer.write(tab(3) + "this.FORMAT_CODE = formatCode;");
            writer.newLine();
            writer.write(tab(3) + "this.CATEGORY = FormatSubCategory.getCategory(formatCode);");
            writer.newLine();
            writer.write(tab(2) + "}");
            writer.newLine();

            writer.newLine();
            writer.write(tab(2) + "public final int getEncodedSize(" + TypeRegistry.getJavaType(name) + " val) throws IOException {");
            writer.newLine();
            writer.write(tab(3) + "if(CATEGORY.WIDTH > 0) {");
            writer.newLine();
            writer.write(tab(4) + "return 1 + CATEGORY.WIDTH + AmqpMarshaller.getEncodedSizeOf" + capFirst(toJavaName(name)) + "(val, this);");
            writer.newLine();
            writer.write(tab(3) + "}");
            writer.newLine();
            writer.write(tab(3) + "return 1;");
            writer.newLine();
            writer.write(tab(2) + "}");
            writer.newLine();

            writer.newLine();
            writer.write(tab(2) + "public final int getEncodedCount(" + TypeRegistry.getJavaType(name) + " val) throws IOException {");
            writer.newLine();
            if (hasCompoundEncoding() || hasArrayEncoding()) {
                writer.write(tab(3) + "return AmqpMarshaller.getEncodedCounfOf" + capFirst(toJavaName(name)) + "(val, this);");
            } else {
                writer.write(tab(3) + "return 1;");
            }
            writer.newLine();
            writer.write(tab(2) + "}");
            writer.newLine();

            writer.newLine();

            writer.write(tab(2) + "public static " + encodingName + " getEncoding(byte formatCode) throws UnsupportedEncodingException {");
            writer.newLine();
            writer.write(tab(3) + "for(" + encodingName + " e: " + encodingName + ".values()) {");
            writer.newLine();
            writer.write(tab(4) + "if(e.FORMAT_CODE == formatCode) {");
            writer.newLine();
            writer.write(tab(5) + "return e;");
            writer.newLine();
            writer.write(tab(4) + "}");
            writer.newLine();
            writer.write(tab(3) + "}");
            writer.newLine();
            writer.write(tab(3) + "throw new UnsupportedEncodingException(\"Unexpected format code for " + capFirst(name) + ": \" + formatCode);");
            writer.newLine();
            writer.write(tab(2) + "}");
            writer.newLine();

            writer.write(tab(1) + "}");
            writer.newLine();
        }
    }

    private boolean writeFields(BufferedWriter writer) throws IOException, UnknownTypeException {
        boolean ret = false;

        if (descriptor != null) {
            ret = true;
            String encodingType = TypeRegistry.resolveAmqpClass(descriptor.getDescribedType()).getJavaType();
            writer.newLine();
            writer.write(tab(1) + "private " + encodingType + " value;");
            writer.newLine();
        }

        for (AmqpField field : fields.values()) {
            ret = true;
            if (field.getDoc() != null) {
                // TODO
            }

            AmqpClass amqpType = field.resolveAmqpFieldType();

            writer.write(tab(1) + "private " + amqpType.getJavaType() + " " + field.getJavaName());

            if (field.getDefaultValue() != null) {
                writer.write(" = " + field.getDefaultValue());
            }

            writer.write(";");
            writer.newLine();
        }

        if (isPrimitive()) {
            writer.newLine();
            writer.write(tab(1) + "private " + TypeRegistry.getJavaType(name) + " value;");
            writer.newLine();

            // If there are multiple possible encodings store the current
            // encoding:
            if (encodings != null && encodings.size() > 1) {
                writer.newLine();
                writer.write(tab(1) + "private " + getEncodingName(true) + " encoding;");
                writer.newLine();
            }
        }
        return ret;
    }

    private boolean writeConstructors(BufferedWriter writer) throws IOException, UnknownTypeException {
        boolean ret = false;

        if (isPrimitive()) {
            writer.newLine();
            writer.write(tab(1) + "public " + typeMapping.getShortName() + "() {");
            writer.newLine();
            writer.write(tab(1) + "}");
            writer.newLine();

            writer.newLine();
            writer.write(tab(1) + "public " + typeMapping.getShortName() + "(" + valueMapping.getJavaType() + " value) {");
            writer.newLine();
            writer.write(tab(2) + "this.value = value;");
            writer.newLine();
            writer.write(tab(1) + "}");
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

            AmqpClass amqpType = field.resolveAmqpFieldType();

            // Setter:
            writer.newLine();
            if (field.getDoc() != null) {
                field.getDoc().writeJavaDoc(writer, 1);
            }
            writer.write(tab(1) + "public final void set" + capFirst(field.getJavaName()) + "(" + amqpType.resolveValueType() + " " + toJavaName(field.getName()) + ") {");
            writer.newLine();
            if (amqpType.isPrimitive() && !amqpType.getName().equals("*")) {
                writer.write(tab(2) + "this." + field.getJavaName() + ".setValue(" + toJavaName(field.getName()) + ");");
            } else {
                writer.write(tab(2) + "this." + field.getJavaName() + " = " + toJavaName(field.getName()) + ";");
            }
            writer.newLine();
            writer.write(tab(1) + "}");
            writer.newLine();
            writer.newLine();
            // Getter:
            if (field.getDoc() != null) {
                field.getDoc().writeJavaDoc(writer, 1);
            }
            writer.write(tab(1) + "public final " + amqpType.resolveValueType() + " get" + capFirst(field.getJavaName()) + "() {");
            writer.newLine();
            if (amqpType.isPrimitive() && !amqpType.getName().equals("*")) {
                writer.write(tab(2) + "return " + field.getJavaName() + ".getValue();");
            } else {
                writer.write(tab(2) + "return " + field.getJavaName() + ";");
            }
            writer.newLine();
            writer.write(tab(1) + "}");
            writer.newLine();
        }

        if (isPrimitive()) {
            String valueType = TypeRegistry.getJavaType(name);

            writer.newLine();
            writer.write(tab(1) + "public final void setValue(" + valueType + " value) {");
            writer.newLine();
            writer.write(tab(2) + "this.value = value;");
            if (hasMultipleEncodings()) {
                writer.newLine();
                writer.write(tab(2) + "this.encoding = null;");
                writer.newLine();
                writer.write(tab(2) + "encodedSize = -1;");
                writer.newLine();
                writer.write(tab(2) + "encodedCount = -1;");
            }
            writer.newLine();
            writer.write(tab(1) + "}");
            writer.newLine();
            writer.newLine();
            // Getter:
            writer.write(tab(1) + "public final " + valueType + " getValue() {");
            writer.newLine();
            writer.write(tab(2) + "return value;");
            writer.newLine();
            writer.write(tab(1) + "}");
            writer.newLine();
        }
        return ret;
    }

    private boolean writeSerializers(BufferedWriter writer) throws IOException, UnknownTypeException {
        boolean ret = false;

        if (!(isPrimitive() || descriptor != null)) {
            return false;
        }

        ret = true;

        writer.newLine();
        writer.write(tab(1) + "public static final " + typeMapping.javaType + " createFromStream(DataInputStream dis) throws IOException {");
        writer.newLine();
        writer.write(tab(2) + typeMapping.javaType + " rc = new " + typeMapping.javaType + "();");
        writer.newLine();
        writer.write(tab(2) + "rc.unmarshal(dis);");
        writer.newLine();
        writer.write(tab(2) + "return rc;");
        writer.newLine();
        writer.write(tab(1) + "}");
        writer.newLine();

        writer.newLine();
        writer.write(tab(1) + "public final void marshal(DataOutputStream dos) throws IOException {");
        writer.newLine();
        writer.write(tab(2) + "marshalConstructor(dos);");
        writer.newLine();
        writer.write(tab(2) + "marshalData(dos);");
        writer.newLine();
        writer.write(tab(1) + "}");
        writer.newLine();

        writer.newLine();
        writer.write(tab(1) + "public final void unmarshal(DataInputStream dis) throws IOException {");
        writer.newLine();
        writer.write(tab(2) + "unmarshalConstructor(dis);");
        writer.newLine();
        writer.write(tab(2) + "unmarshalData(dis);");
        writer.newLine();
        writer.write(tab(1) + "}");
        writer.newLine();

        // Add accessors for primitive encoded sizes:
        if (isPrimitive()) {

            writer.newLine();
            // Handle fixed width encodings:
            if (encodings != null && encodings.size() == 1 && "fixed".equals(encodings.getFirst().getCategory())) {
                AmqpEncoding encoding = encodings.getFirst();

                writer.write(tab(1) + "public final int getEncodedSize() throws IOException{");
                writer.newLine();
                writer.write(tab(2) + "return " + (1 + new Integer(encoding.getWidth())) + ";");
                writer.newLine();
                writer.write(tab(1) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(1) + "public final int getEncodedCount() throws IOException{");
                writer.newLine();
                writer.write(tab(2) + "return 1;");
                writer.newLine();
                writer.write(tab(1) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(1) + "public final void marshalConstructor(DataOutputStream dos) throws IOException {");
                writer.newLine();
                writer.write(tab(2) + "dos.writeByte(FORMAT_CODE);");
                writer.newLine();
                writer.write(tab(1) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(1) + "public final void unmarshalConstructor(DataInputStream dis) throws IOException {");
                writer.newLine();
                writer.write(tab(2) + "byte fc = dis.readByte();");
                writer.newLine();
                writer.write(tab(2) + "if(fc != FORMAT_CODE) {");
                writer.newLine();
                writer.write(tab(3) + "throw new UnsupportedEncodingException(\"Unexpected format code for " + capFirst(toJavaName(name)) + "\" + fc + \" expected: \" + FORMAT_CODE);");
                writer.newLine();
                writer.write(tab(2) + "}");
                writer.newLine();
                writer.write(tab(1) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(1) + "public final void marshalData(DataOutputStream dos) throws IOException {");
                writer.newLine();
                if (hasNonZeroEncoding()) {

                    writer.write(tab(2) + "AmqpMarshaller.write" + capFirst(toJavaName(name)) + "(value, dos);");
                }
                writer.newLine();
                writer.write(tab(1) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(1) + "public final void unmarshalData(DataInputStream dis) throws IOException {");
                writer.newLine();
                if (hasNonZeroEncoding()) {
                    writer.write(tab(2) + "value = AmqpMarshaller.read" + capFirst(toJavaName(name)) + "(dis);");
                }
                writer.newLine();
                writer.write(tab(1) + "}");
                writer.newLine();

            } else {
                writer.write(tab(1) + "public final void chooseEncoding() throws IOException {");
                writer.newLine();
                writer.write(tab(2) + "if(encoding == null) {");
                writer.newLine();
                writer.write(tab(3) + "encoding = AmqpMarshaller.choose" + capFirst(name) + "Encoding(value);");
                writer.newLine();
                writer.write(tab(2) + "}");
                writer.newLine();
                writer.write(tab(1) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(1) + "public final int getEncodedSize() throws IOException {");
                writer.newLine();
                writer.write(tab(2) + "if(encodedSize == -1) {");
                writer.newLine();
                writer.write(tab(3) + "chooseEncoding();");
                writer.newLine();
                writer.write(tab(3) + "encodedSize = encoding.getEncodedSize(value);");
                writer.newLine();
                writer.write(tab(2) + "}");
                writer.newLine();
                writer.write(tab(2) + "return encodedSize;");
                writer.newLine();
                writer.write(tab(1) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(1) + "public final int getEncodedCount() throws IOException {");
                writer.newLine();
                writer.write(tab(2) + "if(encodedCount == -1) {");
                writer.newLine();
                writer.write(tab(3) + "chooseEncoding();");
                writer.newLine();
                writer.write(tab(3) + "encodedCount = encoding.getEncodedCount(value);");
                writer.newLine();
                writer.write(tab(2) + "}");
                writer.newLine();
                writer.write(tab(2) + "chooseEncoding();");
                writer.newLine();
                writer.write(tab(2) + "return encoding.getEncodedCount(value);");
                writer.newLine();
                writer.write(tab(1) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(1) + "public final void marshalConstructor(DataOutputStream dos) throws IOException {");
                writer.newLine();
                writer.write(tab(2) + "chooseEncoding();");
                writer.newLine();
                writer.write(tab(2) + "dos.writeByte(encoding.FORMAT_CODE);");
                writer.newLine();
                writer.write(tab(1) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(1) + "public final void unmarshalConstructor(DataInputStream dis) throws IOException {");
                writer.newLine();
                writer.write(tab(2) + "encoding = " + getEncodingName(false) + ".getEncoding(dis.readByte());");
                writer.newLine();
                writer.write(tab(1) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(1) + "public final void marshalData(DataOutputStream dos) throws IOException {");
                if (hasNonZeroEncoding()) {
                    writer.newLine();
                    writer.write(tab(2) + "encoding.CATEGORY.marshalFormatHeader(this, dos);");
                    writer.newLine();
                    writer.write(tab(2) + "AmqpMarshaller.write" + capFirst(toJavaName(name)) + "(value, encoding, dos);");
                    writer.newLine();
                }
                writer.write(tab(1) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(1) + "public final void unmarshalData(DataInputStream dis) throws IOException {");
                if (hasNonZeroEncoding()) {
                    writer.newLine();
                    writer.write(tab(2) + "value = AmqpMarshaller.read" + capFirst(toJavaName(name)) + "(encoding, encodedSize, encodedCount, dis);");
                    writer.newLine();
                }
                writer.write(tab(1) + "}");
                writer.newLine();
            }
        }

        if (descriptor != null) {

            writer.newLine();
            writer.write(tab(1) + "public final int getEncodedSize() throws IOException {");
            writer.newLine();
            writer.write(tab(2) + "return CONSTRUCTOR.length + value.getEncodedSize();");
            writer.newLine();
            writer.write(tab(1) + "}");
            writer.newLine();

            writer.newLine();
            writer.write(tab(1) + "public final int getEncodedCount() throws IOException {");
            writer.newLine();
            writer.write(tab(2) + "return 1;");
            writer.newLine();
            writer.write(tab(1) + "}");
            writer.newLine();

            writer.newLine();
            writer.write(tab(1) + "public final void marshalConstructor(DataOutputStream dos) throws IOException {");
            writer.newLine();
            writer.write(tab(2) + "dos.write(CONSTRUCTOR);");
            writer.newLine();
            writer.write(tab(2) + "value.marshalConstructor(dos);");
            writer.newLine();
            writer.write(tab(1) + "}");
            writer.newLine();

            writer.newLine();
            writer.write(tab(1) + "public final void marshalData(DataOutputStream dos) throws IOException {");
            writer.newLine();
            writer.write(tab(2) + "value.marshalData(dos);");
            writer.newLine();
            writer.write(tab(1) + "}");
            writer.newLine();

            writer.newLine();
            writer.write(tab(1) + "public final void unmarshalConstructor(DataInputStream dos) throws IOException {");
            writer.newLine();
            writer.write(tab(2) + "//TODO");
            writer.newLine();
            writer.write(tab(1) + "}");
            writer.newLine();

            writer.newLine();
            writer.write(tab(1) + "public final void unmarshalData(DataInputStream dis) throws IOException {");
            writer.newLine();
            if (descriptor.getDescribedType().equals("list")) {
                writer.write(tab(2) + "value = new " + TypeRegistry.resolveAmqpClass(descriptor.getDescribedType()).getJavaType() + "();");
                writer.newLine();
                writer.write(tab(2) + "value.setValue(new ArrayList<AmqpType>(" + fields.size() + "));");
                writer.newLine();
                writer.newLine();
                writer.write(tab(2) + "//Deserialize the values and add them to the list:");
                writer.newLine();
            } else if (descriptor.getDescribedType().equals("map")) {
                writer.write(tab(2) + "value = " + TypeRegistry.resolveAmqpClass(descriptor.getDescribedType()).getJavaType() + ".createFromStream(dis);");
                writer.newLine();
                writer.newLine();
                writer.write(tab(2) + "//Get the values from the map:");
                writer.newLine();
            } else {
                throw new UnknownTypeException("Support for " + descriptor.getDescribedType() + " as a described type isn't yet implemented");
            }

            int f = 0;
            for (AmqpField field : fields.values()) {

                AmqpClass fieldType = field.resolveAmqpFieldType();
                if (descriptor.getDescribedType().equals("list")) {
                    if (fieldType.isRestricted() && fieldType.choice != null) {
                        writer.write(tab(2) + field.getJavaName() + " = " + fieldType.getJavaType() + ".get(" + fieldType.resolveRestrictedType().getTypeMapping().getShortName()
                                + ".createFromStream(dis).getValue());");
                        writer.newLine();
                        writer.write(tab(2) + "value.getValue().set(" + f + ", " + field.getJavaName() + ".getValue());");
                    } else if (field.getType().equals("*")) {
                        writer.newLine();
                        writer.write(tab(2) + field.getJavaName() + " = " + "AmqpMarshaller.readType(dis);");
                        writer.newLine();
                        writer.write(tab(2) + "value.getValue().set(" + f + ", " + field.getJavaName() + ");");
                        writer.newLine();
                    } else {
                        writer.newLine();
                        writer.write(tab(2) + field.getJavaName() + " = new " + fieldType.getTypeMapping().getShortName() + "();");
                        writer.newLine();
                        writer.write(tab(2) + field.getJavaName() + ".unmarshal(dis);");
                        writer.newLine();
                        writer.write(tab(2) + "value.getValue().set(" + f + ", " + field.getJavaName() + ");");
                    }
                    writer.newLine();
                } else if (descriptor.getDescribedType().equals("map")) {
                    if (fieldType.isRestricted() && fieldType.choice != null) {
                        // e.g. value =
                        // AmqpDistributionMode.get(((AmqpUint)value.getValue().get(DISTRIBUTION_MODE_KEY)).getValue());
                        writer.write(tab(2) + field.getJavaName() + " = " + fieldType.getJavaType() + ".get(((" + fieldType.resolveBaseTypeMapping().getJavaType() + ") value.getValue().get("
                                + toJavaConstant(field.getName()) + "_KEY)).getValue());");

                    } else {
                        writer.write(tab(2) + field.getJavaName() + " = (" + fieldType.getJavaType() + ") value.getValue().get(" + toJavaConstant(field.getName()) + "_KEY);");
                    }
                    writer.newLine();
                } else {
                    throw new UnknownTypeException("Support for " + descriptor.getDescribedType() + " as a described type isn't yet implemented");
                }
                f++;
            }

            writer.write(tab(1) + "}");
            writer.newLine();

        }

        return ret;
    }

    public String getJavaType() {
        return typeMapping.getJavaType();
    }

    public String getJavaPackage() {
        return typeMapping.getPackageName();
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

    public AmqpClass resolveRestrictedType() throws UnknownTypeException {
        return TypeRegistry.resolveAmqpClass(restrictedType);
    }

    public void setPrimitive(boolean primitive) {
        this.primitive = primitive;
    }

    public boolean hasMultipleEncodings() {
        return encodings != null && encodings.size() > 1;
    }

    public boolean hasNonFixedEncoding() {
        if (encodings == null) {
            return false;
        }

        for (AmqpEncoding encoding : encodings) {
            if (!encoding.isFixed()) {
                return true;
            }
        }

        return false;
    }

    public boolean hasNonZeroEncoding() {
        if (encodings == null) {
            return false;
        }

        for (AmqpEncoding encoding : encodings) {
            if (Integer.parseInt(encoding.getWidth()) > 0) {
                return true;
            }
        }

        return false;
    }

    public boolean hasVariableEncoding() {
        if (encodings == null) {
            return false;
        }

        for (AmqpEncoding encoding : encodings) {
            if (encoding.isVariable()) {
                return true;
            }
        }

        return false;
    }

    public boolean hasCompoundEncoding() {
        if (encodings == null) {
            return false;
        }

        for (AmqpEncoding encoding : encodings) {
            if (encoding.isCompound()) {
                return true;
            }
        }

        return false;
    }

    public boolean hasArrayEncoding() {
        if (encodings == null) {
            return false;
        }

        for (AmqpEncoding encoding : encodings) {
            if (encoding.isArray()) {
                return true;
            }
        }

        return false;
    }

    public String getEncodingName(boolean full) {
        if (full) {
            return getJavaType() + "." + getEncodingName(false);
        } else {
            return toJavaConstant(name) + "_ENCODING";
        }
    }

    /**
     * Resolves the JavaTypeMapping that will be exposed via the class' api.
     * 
     * @return
     * @throws UnknownTypeException
     */
    public JavaTypeMapping resolveValueMapping() throws UnknownTypeException {
        if (valueMapping != null) {
            return valueMapping;
        }

        if (isPrimitive()) {
            valueMapping = TypeRegistry.getJavaTypeMapping(name);
            return valueMapping;
        }

        if (descriptor != null) {
            String encodingType = descriptor.getSymbolicName();
            encodingType = encodingType.substring(encodingType.lastIndexOf(":") + 1);
            valueMapping = TypeRegistry.resolveAmqpClass(encodingType).typeMapping;
            return valueMapping;
        }

        if (isRestricted()) {
            return typeMapping;
        }

        return null;
    }

    /**
     * Resolves the TypeMapping of this class' base class
     * 
     * @return
     * @throws UnknownTypeException
     */
    public JavaTypeMapping resolveBaseTypeMapping() throws UnknownTypeException {
        if (isRestricted()) {
            return TypeRegistry.resolveAmqpClass(restrictedType).typeMapping;
        } else {
            // Return the any type:
            return TypeRegistry.resolveAmqpClass("*").typeMapping;
        }
    }

    public JavaTypeMapping getTypeMapping() {
        return typeMapping;
    }

    public String resolveValueType() throws UnknownTypeException {
        if (isRestricted()) {
            return typeMapping.getJavaType();
        } else {
            return TypeRegistry.getJavaType(name);
        }
    }

    public String toString() {
        String ret = "Class: " + name + " [Encoding=" + encodings + ", Descriptor=" + descriptor + "]\n";
        ret += " Fields:\n";
        for (AmqpField f : fields.values()) {
            ret += " " + f.toString();
        }

        return ret;
    }

}
