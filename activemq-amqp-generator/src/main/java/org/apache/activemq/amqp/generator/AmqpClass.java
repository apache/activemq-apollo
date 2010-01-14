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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.activemq.amqp.generator.jaxb.schema.Choice;
import org.apache.activemq.amqp.generator.jaxb.schema.Descriptor;
import org.apache.activemq.amqp.generator.jaxb.schema.Doc;
import org.apache.activemq.amqp.generator.jaxb.schema.Encoding;
import org.apache.activemq.amqp.generator.jaxb.schema.Field;
import org.apache.activemq.amqp.generator.jaxb.schema.Type;

public class AmqpClass {

    protected String name;
    protected AmqpDoc doc;
    protected AmqpChoice choice;
    protected AmqpException exception;
    protected AmqpDescriptor descriptor;
    protected LinkedList<AmqpEncoding> encodings;
    protected String restrictedType;

    protected boolean restricted;
    protected boolean primitive;

    LinkedHashMap<String, AmqpField> fields = new LinkedHashMap<String, AmqpField>();
    protected String javaPackage = "";
    protected String javaType = "";
    public boolean handcoded;

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
            javaType = "Amqp" + capFirst(toJavaName(name));
            setPrimitive(true);
        } else {
            javaType = capFirst(toJavaName(name));
        }
        javaPackage = generator.getPackagePrefix() + "." + source;
    }

    public void generate(Generator generator) throws IOException, UnknownTypeException {
        if (handcoded) {
            return;
        }
        String className = getJavaType();

        File file = new File(generator.getOutputDirectory() + File.separator + new String(javaPackage + "." + className).replace(".", File.separator) + ".java");
        file.getParentFile().mkdirs();
        if (file.exists()) {
            file.delete();
        }
        BufferedWriter writer = new BufferedWriter(new FileWriter(file));

        writeJavaCopyWrite(writer);
        writer.write("package " + javaPackage + ";\n");
        writer.newLine();
        if (writeImports(writer, generator)) {
            writer.newLine();
        }

        writer.write("public class " + className);
        if (isPrimitive() || descriptor != null) {
            writer.write(" implements AmqpType");
        }
        writer.write(" {");
        writer.newLine();

        writeConstants(writer);
        
        writeFields(writer);
        
        writeFieldAccesors(writer);
        
        writeSerializers(writer); 
        
        writer.write("}");
        writer.flush();
        writer.close();

    }

    private boolean writeImports(BufferedWriter writer, Generator generator) throws IOException, UnknownTypeException {

        HashSet<String> imports = new HashSet<String>();

        for (AmqpField field : fields.values()) {
            imports.add(field.getJavaPackage());
        }

        if (isPrimitive()) {
            imports.add(TypeRegistry.getJavaPackage(name));
            imports.add("java.io");
            imports.add(generator.getPackagePrefix());
        }

        if (descriptor != null) {
            imports.add(generator.getPackagePrefix());
            imports.add(generator.getPackagePrefix() + ".types" );
            imports.add("java.io");
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
        }

        // Write out encodings:
        if (encodings != null) {
            writeEncodings(writer);
        }

        if (choice != null) {
            ret = true;
            writer.newLine();
            writer.write(tab(1) + "//Constants:");
            writer.newLine();
            for (Map.Entry<String, String> constant : choice.choices.entrySet()) {
                writer.write(tab(1) + "public static final " + TypeRegistry.getJavaType(restrictedType) + " " + Utils.toJavaConstant(constant.getKey()) + " = " + constant.getValue() + ";");
                writer.newLine();
            }
        }
        return ret;
    }

    private void writeEncodings(BufferedWriter writer) throws IOException, UnknownTypeException {
        if (encodings != null && encodings.size() == 1 && "fixed".equals(encodings.getFirst().getCategory())) {
            writer.newLine();
            writer.write(tab(1) + "public static final byte FORMAT_CODE = (byte) " + encodings.getFirst().getCode() + ";");
            writer.newLine();
        } else {
            writer.newLine();
            writer.write(tab(1) + "public static enum " + toJavaConstant(name) + "_ENCODING {");
            writer.newLine();

            int i = 0;
            for (AmqpEncoding encoding : encodings) {
                i++;
                String eName = encoding.getName();
                if (eName == null) {
                    eName = name;
                }
                eName = toJavaConstant(eName);

                writer.write(tab(2) + eName + " ((byte) " + encoding.getCode() + ", " + encoding.getWidth() + ")");
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
            writer.write(tab(2) + "private final byte formatCode;");
            writer.newLine();
            writer.write(tab(2) + "private final int width;");
            writer.newLine();

            // Write constructor:
            writer.newLine();
            writer.write(tab(2) + toJavaConstant(name) + "_ENCODING(byte formatCode, int width) {");
            writer.newLine();
            writer.write(tab(3) + "this.formatCode = formatCode;");
            writer.newLine();
            writer.write(tab(3) + "this.width = width;");
            writer.newLine();
            writer.write(tab(2) + "}");
            writer.newLine();

            // Write Accessors:
            writer.newLine();
            writer.write(tab(2) + "public final byte getFormatCode() {");
            writer.newLine();
            writer.write(tab(3) + "return formatCode;");
            writer.newLine();
            writer.write(tab(2) + "}");
            writer.newLine();

            writer.newLine();
            writer.write(tab(2) + "public final int getWidth() {");
            writer.newLine();
            writer.write(tab(3) + "return width;");
            writer.newLine();
            writer.write(tab(2) + "}");
            writer.newLine();

            writer.newLine();
            writer.write(tab(2) + "public final int getEncodedSize(" + TypeRegistry.getJavaType(name) + " val) throws IOException{");
            writer.newLine();
            writer.write(tab(3) + "if(width > 0) {");
            writer.newLine();
            writer.write(tab(4) + "return 1 + width + AmqpMarshaller.getEncodedSizeOf" + capFirst(toJavaName(name)) + "(val, this);");
            writer.newLine();
            writer.write(tab(3) + "}");
            writer.newLine();
            writer.write(tab(3) + "return 1;");
            writer.newLine();
            writer.write(tab(2) + "}");
            writer.newLine();
            writer.write(tab(1) + "}");
            writer.newLine();
        }
    }

    private boolean writeFields(BufferedWriter writer) throws IOException, UnknownTypeException {
        boolean ret = false;
        
        if (descriptor != null)
        {
            ret = true;
            String encodingType = descriptor.getSymbolicName();
            encodingType = encodingType.substring(encodingType.lastIndexOf(":") + 1);
            encodingType = "Amqp" + capFirst(toJavaName(encodingType));
            
            writer.newLine();
            writer.write(tab(1) + "private " + encodingType + " value;");
            writer.newLine();
        }
        
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

    private boolean writeFieldAccesors(BufferedWriter writer) throws IOException, UnknownTypeException {
        boolean ret = false;
        
        for (AmqpField field : fields.values()) {
            ret = true;
            if (field.getDoc() != null) {
                // TODO
            }

            String array = field.isMultiple() ? " []" : "";

            // Setter:
            writer.newLine();
            writer.write(tab(1) + "public final void set" + capFirst(field.getJavaName()) + "(" + field.getJavaType() + array + " " + toJavaName(field.getName()) + ") {");
            writer.newLine();
            writer.write(tab(2) + "this." + field.getJavaName() + " = " + field.getJavaName() + ";");
            writer.newLine();
            writer.write(tab(1) + "}");
            writer.newLine();
            writer.newLine();
            // Getter:
            writer.write(tab(1) + "public final " + field.getJavaType() + array + " get" + capFirst(field.getJavaName()) + "() {");
            writer.newLine();
            writer.write(tab(2) + "return " + field.getJavaName() + ";");
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

        // Add accessors for primitive encoded sizes:
        if (isPrimitive()) {
            ret = true;
            writer.newLine();
            writer.write(tab(1) + "public final int getEncodedSize() throws IOException{");
            writer.newLine();
            // Handle fixed width encodings:
            if (encodings != null && encodings.size() == 1 && "fixed".equals(encodings.getFirst().getCategory())) {
                AmqpEncoding encoding = encodings.getFirst();

                writer.write(tab(2) + "return " + (1 + new Integer(encoding.getWidth())) + ";");
                writer.newLine();
                writer.write(tab(1) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(1) + "public final void marshal(DataOutputStream dos) throws IOException {");
                writer.newLine();
                writer.write(tab(2) + "dos.writeByte(FORMAT_CODE);");
                writer.newLine();
                if (Integer.parseInt(encoding.getWidth()) > 0) {
                    writer.write(tab(2) + "AmqpMarshaller.write" + capFirst(toJavaName(name)) + "(value, dos);");
                    writer.newLine();
                }
                writer.write(tab(1) + "}");
                writer.newLine();
            } else {
                writer.newLine();
                writer.write(tab(2) + "chooseEncoding();");
                writer.newLine();
                writer.write(tab(2) + "return encoding.getEncodedSize(value);");
                writer.newLine();
                writer.write(tab(1) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(1) + "public final void marshal(DataOutputStream dos) throws IOException {");
                writer.newLine();
                writer.write(tab(2) + "chooseEncoding();");
                writer.newLine();
                writer.write(tab(2) + "dos.writeByte(encoding.getFormatCode());");
                if (hasNonZeroEncoding()) {
                    writer.newLine();
                    writer.write(tab(2) + "int size = encoding.getEncodedSize(value);");
                    writer.newLine();
                    writer.write(tab(2) + "if(encoding.getWidth() == 1) {");
                    writer.newLine();
                    writer.write(tab(3) + "dos.writeByte(size);");
                    writer.newLine();
                    writer.write(tab(2) + "}");
                    writer.newLine();
                    writer.write(tab(2) + "else if(encoding.getWidth() == 4) {");
                    writer.newLine();
                    writer.write(tab(3) + "dos.writeInt(size);");
                    writer.newLine();
                    writer.write(tab(2) + "}");
                    writer.newLine();
                    writer.write(tab(2) + "AmqpMarshaller.write" + capFirst(toJavaName(name)) + "(value, encoding, dos);");
                    writer.newLine();
                }

                writer.write(tab(1) + "}");
                writer.newLine();

                writer.newLine();
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
            writer.write(tab(1) + "public final void marshal(DataOutputStream dos) throws IOException {");
            writer.newLine();
            writer.write(tab(2) + "dos.write(CONSTRUCTOR);");
            writer.newLine();
            writer.write(tab(2) + "value.marshal(dos);");
            writer.newLine();
            writer.write(tab(1) + "}");
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

    public String getEncodingName(boolean full) {
        if (full) {
            return getJavaType() + "." + getEncodingName(false);
        } else {
            return toJavaConstant(name) + "_ENCODING";
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
