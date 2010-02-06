package org.apache.activemq.amqp.generator;

import static org.apache.activemq.amqp.generator.Utils.capFirst;
import static org.apache.activemq.amqp.generator.Utils.padHex;
import static org.apache.activemq.amqp.generator.Utils.tab;
import static org.apache.activemq.amqp.generator.Utils.toJavaConstant;
import static org.apache.activemq.amqp.generator.Utils.toJavaName;
import static org.apache.activemq.amqp.generator.Utils.writeJavaComment;
import static org.apache.activemq.amqp.generator.Utils.writeJavaCopyWrite;

import java.io.BufferedWriter;
import java.io.DataInput;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.rmi.UnexpectedException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.activemq.amqp.generator.TypeRegistry.JavaTypeMapping;
import org.apache.activemq.amqp.generator.jaxb.schema.Amqp;
import org.apache.activemq.amqp.generator.jaxb.schema.Choice;
import org.apache.activemq.amqp.generator.jaxb.schema.Descriptor;
import org.apache.activemq.amqp.generator.jaxb.schema.Doc;
import org.apache.activemq.amqp.generator.jaxb.schema.Encoding;
import org.apache.activemq.amqp.generator.jaxb.schema.Field;
import org.apache.activemq.amqp.generator.jaxb.schema.Section;
import org.apache.activemq.amqp.generator.jaxb.schema.Type;

public class AmqpClass {

    protected String name;
    protected String label;
    protected AmqpDoc doc = new AmqpDoc();
    protected AmqpChoice choice;
    protected AmqpException exception;
    protected AmqpDescriptor descriptor;
    protected LinkedList<AmqpEncoding> encodings;
    protected String restrictedType;

    protected boolean restricted;
    protected boolean primitive;
    protected boolean isCommand;

    LinkedHashMap<String, AmqpField> fields = new LinkedHashMap<String, AmqpField>();
    public boolean handcoded;

    // Java mapping for this class:
    protected TypeRegistry.JavaTypeMapping typeMapping;
    // Java mapping of the value that this type holds (if any)
    protected TypeRegistry.JavaTypeMapping valueMapping;
    // Java mapping of the value that this type holds (if any)
    protected TypeRegistry.JavaTypeMapping beanMapping;

    public TypeRegistry.JavaTypeMapping versionMarshaller;

    public void parseFromType(Generator generator, Amqp source, Section section, Type type) throws UnknownTypeException {
        this.name = type.getName();
        this.restrictedType = type.getSource();
        this.label = type.getLabel();
        isCommand = Generator.COMMANDS.contains(name) || Generator.CONTROLS.contains(name);

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

        if (label != null) {
            doc.setLabel("Represents a " + label);
        }

        if (type.getClazz().equalsIgnoreCase("primitive")) {
            setPrimitive(true);
        }

        // See if this is a restricting type (used to restrict the type of a
        // field):
        if (type.getClazz().equalsIgnoreCase("restricted")) {
            this.restricted = true;
        }

        typeMapping = new JavaTypeMapping(name, generator.getPackagePrefix() + ".types." + "Amqp" + capFirst(toJavaName(name)));

        if (needsMarshaller()) {
            beanMapping = new JavaTypeMapping(name + "-bean", generator.getPackagePrefix() + ".marshaller." + typeMapping + "Bean");
        }

        // For described types the value is actually the bean mapping which
        // describes all of the fields:
        if (isDescribed()) {
            valueMapping = beanMapping;
        } else if (isPrimitive()) {
            valueMapping = TypeRegistry.getJavaTypeMapping(name);
        } else if (isRestricted()) {
            valueMapping = typeMapping;
        }
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
        if (writeImports(writer, generator, false)) {
            writer.newLine();
        }

        if (doc != null) {
            doc.writeJavaDoc(writer, 0);
        } else if (label != null) {
            writeJavaComment(writer, 0, "Represents a " + label);
        }

        // We use enums for restricted types with a choice:
        if (isEnumType()) {
            writer.write("public enum " + className);
        } else if (isMarshallable()) {

            if (isRestricted()) {
                writer.write("public class " + className + " extends " + resolveRestrictedType().getTypeMapping());
            } else {
                writer.write("public class " + className + " extends AmqpType<" + getValueMapping() + "> implements " + beanMapping);
            }

            if (isCommand()) {
                writer.write(", AmqpCommand");
            }
        }

        writer.write(" {");
        writer.newLine();

        if (isMarshallable()) {
            writer.newLine();
            writeBeanImpl(writer, 1);
        } else {
            writeEnumType(writer);
        }

        writer.write("}");
        writer.flush();
        writer.close();

        // We don't generate beans or marshallers for restricted types:
        if (!isRestricted()) {
            generateBeanInterface(generator);
            generateMarshaller(generator);
        }

    }

    private boolean writeImports(BufferedWriter writer, Generator generator, boolean marshaller) throws IOException, UnknownTypeException {

        TreeSet<String> imports = new TreeSet<String>();
        for (AmqpField field : fields.values()) {

            AmqpClass fieldType = field.resolveAmqpFieldType();
            filterOrAddImport(imports, fieldType.getTypeMapping(), marshaller);
            filterOrAddImport(imports, fieldType.getValueMapping(), marshaller);

            if (fieldType.choice != null) {
                filterOrAddImport(imports, fieldType.resolveBaseTypeMapping(), marshaller);
            }

            if (marshaller) {
                JavaTypeMapping mapping = fieldType.getTypeMapping();
                if (fieldType.isRestricted()) {
                    mapping = fieldType.resolveBaseTypeMapping();
                }
                if (!mapping.getAmqpType().equals("*")) {
                    imports.add(mapping.getFullVersionMarshallerName(generator));

                }

            }
        }

        if (!marshaller && isCommand()) {
            imports.add(generator.getPackagePrefix() + ".AmqpCommandHandler");
            imports.add(generator.getPackagePrefix() + ".AmqpCommand");
        }

        if (marshaller) {
            // Add the marshalled type:
            filterOrAddImport(imports, typeMapping, marshaller);

            if (hasCompoundEncoding()) {
                filterOrAddImport(imports, TypeRegistry.resolveAmqpClass("*").getTypeMapping(), marshaller);
            }
            imports.add("java.io.DataInput");
            TypeRegistry.resolveAmqpClass("null").getTypeMapping().getFullVersionMarshallerName(generator);
            imports.add(generator.getMarshallerPackage() + ".Encoder");
            imports.add(generator.getMarshallerPackage() + ".Encoder.*");
            imports.add("org.apache.activemq.util.buffer.Buffer");

            imports.add(generator.getPackagePrefix() + ".marshaller.UnexpectedTypeException");
            imports.add(generator.getPackagePrefix() + ".marshaller.AmqpEncodingError");

            if (descriptor != null || hasMultipleEncodings()) {
                imports.add(generator.getPackagePrefix() + ".marshaller.Encoded");
            }

            if (descriptor != null) {

                imports.add(generator.getPackagePrefix() + ".marshaller.AmqpEncodingError");

                AmqpClass describedType = descriptor.resolveDescribedType();
                if (describedType.getName().equals("list")) {
                    imports.add(TypeRegistry.resolveAmqpClass("list").getTypeMapping().getFullVersionMarshallerName(generator) + ".*");
                    imports.add(TypeRegistry.resolveAmqpClass("list").getValueMapping().getImport());
                } else if (describedType.getName().equals("map")) {
                    imports.add(TypeRegistry.resolveAmqpClass("map").getTypeMapping().getFullVersionMarshallerName(generator) + ".*");
                    imports.add(TypeRegistry.resolveAmqpClass("map").getValueMapping().getImport());
                    imports.add(TypeRegistry.resolveAmqpClass("symbol").getTypeMapping().getFullVersionMarshallerName(generator));
                    // Import symbol which is used for the keys:
                    imports.add(TypeRegistry.resolveAmqpClass("symbol").getTypeMapping().getImport());
                    imports.add("java.util.Map");
                }

                filterOrAddImport(imports, describedType.getTypeMapping(), marshaller);

                imports.add(generator.getPackagePrefix() + ".types.AmqpLong");
                imports.add(generator.getPackagePrefix() + ".types.AmqpSymbol");
                // filterOrAddImport(imports,
                // describedType.resolveValueMapping());
            }

            imports.add(generator.getPackagePrefix() + ".marshaller.Encoded");
            imports.add(generator.getPackagePrefix() + ".marshaller.Encoding");
            imports.add(generator.getPackagePrefix() + ".marshaller.AmqpVersion");
            imports.add(generator.getPackagePrefix() + ".types.AmqpType");

            imports.add(beanMapping.getImport());
            imports.add("java.io.DataOutput");
            imports.add("java.io.IOException");

        } else if (isMarshallable()) {
            imports.add(generator.getPackagePrefix() + ".marshaller.Encoded");
            if (isRestricted()) {
                imports.add(resolveRestrictedType().getBeanMapping().getImport());
                imports.add(resolveRestrictedType().valueMapping.getImport());
            } else {
                imports.add(generator.getPackagePrefix() + ".marshaller.AmqpEncodingError");
                imports.add(generator.getPackagePrefix() + ".marshaller.AmqpMarshaller");
                imports.add(beanMapping.getImport());
                imports.add("java.io.IOException");
            }
        }

        if (isDescribed()) {
            filterOrAddImport(imports, getValueMapping(), marshaller);
        }

        if (isRestricted()) {
            if (choice != null) {
                imports.add(generator.getPackagePrefix() + ".marshaller.AmqpEncodingError");
                imports.add("java.util.HashMap");
            }
            imports.add(TypeRegistry.resolveAmqpClass(restrictedType).getTypeMapping().getImport());

        }

        if (isPrimitive()) {
            filterOrAddImport(imports, getValueMapping(), marshaller);
        }

        boolean ret = false;

        for (String toImport : imports) {
            ret = true;
            writer.write("import " + toImport + ";");
            writer.newLine();
        }
        return ret;
    }

    private void filterOrAddImport(TreeSet<String> imports, JavaTypeMapping mapping, boolean marshaller) {
        if (mapping == null) {
            return;
        }
        if (mapping.getImport() == null) {
            return;
        }

        if (!marshaller) {
            if (mapping.getPackageName().equals(typeMapping.getPackageName())) {
                return;
            }
        }
        imports.add(mapping.getImport());
    }

    private void writeEnumType(BufferedWriter writer) throws IOException, UnknownTypeException {

        if (isEnumType()) {
            writer.newLine();
            int i = 0;
            AmqpClass amqpClass = TypeRegistry.resolveAmqpClass(restrictedType);
            JavaTypeMapping amqpType = amqpClass.getTypeMapping();
            JavaTypeMapping valueType = amqpClass.getValueMapping();

            for (Choice constant : choice.choices) {
                i++;
                if (constant.getDoc() != null) {
                    new AmqpDoc(constant.getDoc()).writeJavaDoc(writer, 1);
                }

                writer.write(tab(1) + toJavaConstant(constant.getName()) + "(new " + valueType + "(\"" + constant.getValue() + "\"))");
                if (i < choice.choices.size()) {
                    writer.write(",");
                } else {
                    writer.write(";");
                }
                writer.newLine();
            }

            writer.newLine();
            writer.write(tab(1) + "private static final HashMap<" + valueType + ", " + getJavaType() + "> LOOKUP = new HashMap<" + valueType + ", " + getJavaType() + ">(2);");
            writer.newLine();
            writer.write(tab(1) + "static {");
            writer.newLine();
            writer.write(tab(2) + "for (" + getJavaType() + " " + toJavaName(getName()) + " : " + getJavaType() + ".values()) {");
            writer.newLine();
            writer.write(tab(3) + "LOOKUP.put(" + toJavaName(getName()) + ".value.getValue(), " + toJavaName(getName()) + ");");
            writer.newLine();
            writer.write(tab(2) + "}");
            writer.newLine();
            writer.write(tab(1) + "}");
            writer.newLine();

            writer.newLine();
            writer.write(tab(1) + "private final " + amqpType + " value;");
            writer.newLine();

            writer.newLine();
            writer.write(tab(1) + "private " + typeMapping + "(" + valueType + " value) {");
            writer.newLine();
            writer.write(tab(2) + "this.value = new " + amqpType + "(value);");

            writer.newLine();
            writer.write(tab(1) + "}");
            writer.newLine();

            writer.newLine();
            writer.write(tab(1) + "public final " + amqpType + " getValue() {");
            writer.newLine();
            writer.write(tab(2) + "return value;");
            writer.newLine();
            writer.write(tab(1) + "}");
            writer.newLine();

            writer.newLine();
            writer.write(tab(1) + "public static final " + getJavaType() + " get(" + amqpType + " value) throws AmqpEncodingError{");
            writer.newLine();
            writer.write(tab(2) + getJavaType() + " " + toJavaName(getName()) + "= LOOKUP.get(value.getValue());");
            writer.newLine();
            writer.write(tab(2) + "if (" + toJavaName(getName()) + " == null) {");
            writer.newLine();
            writer.write(tab(3) + "//TODO perhaps this should be an IllegalArgumentException?");
            writer.newLine();
            writer.write(tab(3) + "throw new AmqpEncodingError(\"Unknown " + toJavaName(getName()) + ": \" + value + \" expected one of \" + LOOKUP.keySet());");
            writer.newLine();
            writer.write(tab(2) + "}");
            writer.newLine();
            writer.write(tab(2) + "return " + toJavaName(getName()) + ";");
            writer.newLine();
            writer.write(tab(1) + "}");
            writer.newLine();

            // writer.newLine();
            // writer.write(tab(1) + "public static final  " +
            // getTypeMapping().getJavaType() +
            // " createFromStream(DataInput dis) throws IOException {");
            // writer.newLine();
            // writer.write(tab(2) + " return get(" +
            // amqpType.getTypeMapping().getJavaType() +
            // ".createFromStream(dis)).getValue();");
            // writer.newLine();
            // writer.write(tab(1) + "}");
            // writer.newLine();

        }
    }

    private void writeEncodings(BufferedWriter writer) throws IOException, UnknownTypeException {
        if (isDescribed()) {

            JavaTypeMapping describedType = descriptor.resolveDescribedType().getTypeMapping();

            if (descriptor.getDescribedType().equals("list")) {
                writer.newLine();
                writer.write(tab(1) + "private static final ListDecoder DECODER = new ListDecoder() {");
                writer.newLine();
                writer.write(tab(2) + "public final AmqpType<?> unmarshalType(int pos, DataInput in) throws IOException {");
                writer.newLine();
                writer.write(tab(3) + "switch(pos) {");
                writer.newLine();
                int f = 0;
                for (AmqpField field : fields.values()) {
                    AmqpClass fieldType = field.resolveAmqpFieldType();

                    writer.write(tab(3) + "case " + f++ + ": {");
                    writer.newLine();
                    if (fieldType.isEnumType() || fieldType.isAny()) {
                        writer.write(tab(4) + "return " + fieldType.getMarshaller() + ".unmarshalType(in);");
                    } else {
                        writer.write(tab(4) + "return " + fieldType.getMarshaller() + ".unmarshalType(new " + fieldType.getTypeMapping() + "(), in);");
                    }
                    writer.newLine();
                    writer.write(tab(3) + "}");
                    writer.newLine();
                }
                writer.write(tab(3) + "default: {");
                writer.newLine();
                writer.write(tab(4) + "return AmqpMarshaller.SINGLETON.unmarshalType(in);");
                writer.newLine();
                writer.write(tab(3) + "}");
                writer.newLine();
                writer.write(tab(3) + "}");
                writer.newLine();
                writer.write(tab(2) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(2) + "public final AmqpType<?> decodeType(int pos, EncodedBuffer buffer) throws AmqpEncodingError {");
                writer.newLine();
                writer.write(tab(3) + "switch(pos) {");
                writer.newLine();

                f = 0;
                for (AmqpField field : fields.values()) {
                    AmqpClass fieldType = field.resolveAmqpFieldType();

                    writer.write(tab(3) + "case " + f++ + ": {");
                    writer.newLine();
                    if (fieldType.isEnumType() || fieldType.isAny() || isPrimitive()) {
                        writer.write(tab(4) + "return " + fieldType.getMarshaller() + ".decodeType(buffer);");
                    } else {
                        writer.write(tab(4) + "return " + fieldType.getMarshaller() + ".decodeType(new " + fieldType.getTypeMapping() + "(), buffer);");
                    }
                    writer.newLine();
                    writer.write(tab(3) + "}");
                    writer.newLine();
                }
                writer.write(tab(3) + "default: {");
                writer.newLine();
                writer.write(tab(4) + "return AmqpMarshaller.SINGLETON.decodeType(buffer);");
                writer.newLine();
                writer.write(tab(3) + "}");
                writer.newLine();
                writer.write(tab(3) + "}");
                writer.newLine();
                writer.write(tab(2) + "}");
                writer.newLine();

                writer.write(tab(1) + "};");
            } else if (descriptor.getDescribedType().equals("map")) {

                writer.newLine();
                writer.write(tab(1) + "private static final MapDecoder DECODER = new MapDecoder() {");
                writer.newLine();
                writer.write(tab(2) + "public void decodeToMap(EncodedBuffer encodedKey, EncodedBuffer encodedValue, Map<AmqpType<?>, AmqpType<?>> map) throws AmqpEncodingError {");
                writer.newLine();
                writer.write(tab(3) + "AmqpSymbol key = AmqpSymbolMarshaller.decodeType(encodedKey);");
                writer.newLine();
                writer.write(tab(3) + "if (key == null) {");
                writer.newLine();
                writer.write(tab(4) + "throw new AmqpEncodingError(\"Null Key for \" + SYMBOLIC_ID);");
                writer.newLine();
                writer.write(tab(3) + "}");
                writer.newLine();
                writer.newLine();
                int f = 0;
                for (AmqpField field : fields.values()) {
                    AmqpClass fieldType = field.resolveAmqpFieldType();
                    writer.write(tab(3) + (f > 0 ? "else " : "") + "if (key.getValue().equals(" + toJavaConstant(field.getName()) + "_KEY.getValue())){");
                    writer.newLine();
                    if (fieldType.isEnumType() || fieldType.isAny() || fieldType.isPrimitive()) {
                        writer.write(tab(4) + "map.put(" + toJavaConstant(field.getName()) + "_KEY, " + fieldType.getMarshaller() + ".decodeType(encodedValue));");
                    } else {
                        writer.write(tab(4) + "map.put(" + toJavaConstant(field.getName()) + "_KEY, " + fieldType.getMarshaller() + ".decodeType(new " + fieldType.getTypeMapping()
                                + "(), encodedValue));");
                    }
                    writer.newLine();
                    writer.write(tab(3) + "}");
                    writer.newLine();
                }
                writer.write(tab(3) + "else {");
                writer.newLine();
                writer.write(tab(4) + "throw new UnexpectedTypeException(\"Invalid Key for \" + SYMBOLIC_ID + \" : \" + key);");
                writer.newLine();
                writer.write(tab(3) + "}");
                writer.newLine();
                writer.write(tab(2) + "}");
                writer.newLine();

                writer.write(tab(2) + "public void unmarshalToMap(DataInput in, Map<AmqpType<?>, AmqpType<?>> map) throws AmqpEncodingError, IOException {");
                writer.newLine();
                writer.write(tab(3) + "AmqpSymbol key = AmqpSymbolMarshaller.unmarshalType(in);");
                writer.newLine();
                writer.write(tab(3) + "if (key == null) {");
                writer.newLine();
                writer.write(tab(4) + "throw new AmqpEncodingError(\"Null Key for \" + SYMBOLIC_ID);");
                writer.newLine();
                writer.write(tab(3) + "}");
                writer.newLine();
                writer.newLine();
                f = 0;
                for (AmqpField field : fields.values()) {
                    AmqpClass fieldType = field.resolveAmqpFieldType();
                    writer.write(tab(3) + (f > 0 ? "else " : "") + "if (key.getValue().equals(" + toJavaConstant(field.getName()) + "_KEY.getValue())){");
                    writer.newLine();
                    if (fieldType.isEnumType() || fieldType.isAny() || fieldType.isPrimitive()) {
                        writer.write(tab(4) + "map.put(" + toJavaConstant(field.getName()) + "_KEY, " + fieldType.getMarshaller() + ".unmarshalType(in));");
                    } else {
                        writer.write(tab(4) + "map.put(" + toJavaConstant(field.getName()) + "_KEY, " + fieldType.getMarshaller() + ".unmarshalType(new " + fieldType.getTypeMapping() + "(), in));");
                    }
                    writer.newLine();
                    writer.write(tab(3) + "}");
                    writer.newLine();
                }
                writer.write(tab(3) + "else {");
                writer.newLine();
                writer.write(tab(4) + "throw new UnexpectedTypeException(\"Invalid Key for \" + SYMBOLIC_ID + \" : \" + key);");
                writer.newLine();
                writer.write(tab(3) + "}");
                writer.newLine();
                writer.write(tab(2) + "}");
                writer.newLine();

                writer.write(tab(1) + "};");
                writer.newLine();
            } else {
                throw new UnexpectedException("Unsupported described type: " + descriptor.getDescribedType());
            }

            writer.newLine();
            writer.write(tab(1) + "public static class " + getJavaType() + "Encoded extends DescribedEncoded<" + getValueMapping() + "> implements " + beanMapping.getJavaType() + "{");
            writer.newLine();

            // Write out fields:
            writer.newLine();
            writer.write(tab(2) + "private " + describedType + " fields;");
            writer.newLine();
            writeFields(writer, 2);
            writer.newLine();

            writer.newLine();
            writer.write(tab(2) + "public " + getJavaType() + "Encoded(DescribedBuffer buffer) {");
            writer.newLine();
            writer.write(tab(3) + "super(buffer);");
            writer.newLine();
            writer.write(tab(2) + "}");
            writer.newLine();

            writer.newLine();
            writer.write(tab(2) + "public " + getJavaType() + "Encoded(" + getJavaType() + " value) {");
            writer.newLine();
            writer.write(tab(3) + "super(value);");
            writer.newLine();
            writer.write(tab(3) + "fields = new " + describedType + "();");
            writer.newLine();
            writer.write(tab(2) + "}");
            writer.newLine();
            writer.newLine();
            writer.write(tab(2) + "protected final int getDescriptorSize() {");
            writer.newLine();
            writer.write(tab(3) + "return DESCRIPTOR.length;");
            writer.newLine();
            writer.write(tab(2) + "}");
            writer.newLine();

            writer.newLine();
            writer.write(tab(2) + "protected final void marshalDescriptor(DataOutput out) throws IOException {");
            writer.newLine();
            writer.write(tab(3) + "out.write(DESCRIPTOR);");
            writer.newLine();
            writer.write(tab(2) + "}");
            writer.newLine();

            writer.newLine();
            writer.write(tab(2) + "protected final void encodeDescriptor(Buffer target, int offset) throws AmqpEncodingError {");
            writer.newLine();
            writer.write(tab(3) + "System.arraycopy(DESCRIPTOR, 0, target.data, target.offset + offset, DESCRIPTOR.length);");
            writer.newLine();
            writer.write(tab(2) + "}");
            writer.newLine();

            writer.newLine();
            writer.write(tab(2) + "protected final String getSymbolicId() {");
            writer.newLine();
            writer.write(tab(3) + "return SYMBOLIC_ID;");
            writer.newLine();
            writer.write(tab(2) + "}");
            writer.newLine();

            writer.newLine();
            writer.write(tab(2) + "protected final long getNumericId() {");
            writer.newLine();
            writer.write(tab(3) + "return NUMERIC_ID;");
            writer.newLine();
            writer.write(tab(2) + "}");
            writer.newLine();

            // Write out the field accessors:
            writer.newLine();
            writeFieldAccesors(writer, 2, false);
            writer.newLine();

            writer.newLine();
            writer.write(tab(2) + "protected final void encodeDescribed(" + getValueMapping().getJavaType() + " value, Buffer encoded, int offset) throws AmqpEncodingError {");
            writer.newLine();
            writer.write(tab(3) + "Buffer buffer = fields.getEncoded(AmqpMarshaller.SINGLETON).getBuffer();");
            writer.newLine();
            writer.write(tab(3) + "System.arraycopy(buffer, buffer.offset, encoded, encoded.offset + offset, buffer.length);");
            writer.newLine();
            writer.write(tab(2) + "}");
            writer.newLine();

            writer.newLine();
            writer.write(tab(2) + "protected final " + getValueMapping().getJavaType() + " decodeDescribed(EncodedBuffer encoded) throws AmqpEncodingError {");
            writer.newLine();
            writer.write(tab(3) + describedType + "Encoded encodedType = " + descriptor.resolveDescribedType().getMarshaller() + ".createEncoded(encoded);");
            writer.newLine();
            writer.write(tab(3) + "encodedType.setDecoder(DECODER);");
            writer.newLine();
            writer.write(tab(3) + "fields = new " + describedType + "((Encoded<" + descriptor.resolveDescribedType().getValueMapping() + ">)encodedType);");
            writer.newLine();
            writer.write(tab(3) + "return this;");
            writer.newLine();
            writer.write(tab(2) + "}");
            writer.newLine();

            writer.newLine();
            writer.write(tab(2) + "protected final void marshalDescribed(DataOutput out) throws IOException {");
            writer.newLine();
            writer.write(tab(3) + "fields.marshal(out, AmqpMarshaller.SINGLETON);");
            writer.newLine();
            writer.write(tab(2) + "}");
            writer.newLine();

            writer.newLine();
            writer.write(tab(2) + "protected final " + getValueMapping().getJavaType() + " unmarshalDescribed(DataInput in) throws IOException {");
            writer.newLine();
            writer.write(tab(3) + "//TODO should actually be attempting to directly unmarshal the data here, without copying to intermediate buffer:");
            writer.newLine();
            writer.write(tab(3) + "decodeDescribed(FormatCategory.createBuffer(in.readByte(), in));");
            writer.newLine();
            writer.write(tab(3) + "return this;");
            writer.newLine();
            writer.write(tab(2) + "}");
            writer.newLine();

            writer.write(tab(1) + "}");
            writer.newLine();

            return;
        }

        if (!hasMultipleEncodings() && !hasNonFixedEncoding()) {
            writer.newLine();
            writer.write(tab(1) + "public static final byte FORMAT_CODE = (byte) " + encodings.getFirst().getCode() + ";");
            writer.newLine();
            writer.write(tab(1) + "public static final FormatSubCategory FORMAT_CATEGORY  = FormatSubCategory.getCategory(FORMAT_CODE);");
            writer.newLine();
            // writer.write(tab(1) + "public static final " + getJavaType() +
            // "Encoded ENCODING = new " + getJavaType() + "Encoded();");
            // writer.newLine();

            writer.newLine();
            writer.write(tab(1) + "public static class " + getJavaType() + "Encoded  extends AbstractEncoded<" + getValueMapping().getJavaType() + "> implements " + beanMapping + "{");
            writer.newLine();

            writer.newLine();
            writer.write(tab(2) + "public " + getJavaType() + "Encoded (EncodedBuffer encoded) {");
            writer.newLine();
            writer.write(tab(3) + "super(encoded);");
            writer.newLine();
            writer.write(tab(2) + "}");
            writer.newLine();

            writer.newLine();
            writer.write(tab(2) + "public " + getJavaType() + "Encoded (" + getValueMapping().getJavaType() + " value) throws AmqpEncodingError {");
            writer.newLine();
            writer.write(tab(3) + "super(FORMAT_CODE, value);");
            writer.newLine();
            writer.write(tab(2) + "}");
            writer.newLine();

            writer.newLine();
            writer.write(tab(2) + "public final void encode(" + getValueMapping().getJavaType() + " value, Buffer encoded, int offset) throws AmqpEncodingError{");
            writer.newLine();
            if (hasNonZeroEncoding()) {
                writer.write(tab(3) + "ENCODER.encode" + capFirst(toJavaName(name)) + "(value, encoded, offset);");
            }
            writer.newLine();
            writer.write(tab(2) + "}");
            writer.newLine();

            writer.newLine();
            writer.write(tab(2) + "public final " + getValueMapping().getJavaType() + " decode(EncodedBuffer encoded) throws AmqpEncodingError{");
            writer.newLine();
            if (hasNonZeroEncoding()) {
                writer.write(tab(3) + "return ENCODER.decode" + capFirst(toJavaName(name)) + "(encoded.getBuffer(), encoded.getDataOffset());");
            } else {
                writer.write(tab(3) + "return ENCODER.valueOf" + capFirst(toJavaName(name)) + "();");
            }
            writer.newLine();
            writer.write(tab(2) + "}");
            writer.newLine();

            writer.newLine();
            writer.write(tab(2) + "public final void marshalData(DataOutput out) throws IOException {");
            writer.newLine();
            if (hasNonZeroEncoding()) {
                writer.write(tab(3) + "ENCODER.write" + capFirst(toJavaName(name)) + "(value, out);");
            }
            writer.newLine();
            writer.write(tab(2) + "}");
            writer.newLine();

            writer.newLine();
            writer.write(tab(2) + "public final " + getValueMapping().getJavaType() + " unmarshalData(DataInput in) throws IOException {");
            writer.newLine();
            if (hasNonZeroEncoding()) {
                writer.write(tab(3) + "return ENCODER.read" + capFirst(toJavaName(name)) + "(in);");
            } else {
                writer.write(tab(3) + "return ENCODER.valueOf" + capFirst(toJavaName(name)) + "();");
            }
            writer.newLine();
            writer.write(tab(2) + "}");
            writer.newLine();

            writer.write(tab(1) + "}");
        } else {
            String encodingName = getEncodingName(false);

            writer.newLine();
            for (AmqpEncoding encoding : encodings) {
                writer.write(tab(1) + "public static final byte " + toJavaConstant(encoding.getName()) + "_FORMAT_CODE = (byte) " + encoding.getCode() + ";");
                writer.newLine();
            }

            // Create an enum that captures the allowed encodings:
            writer.newLine();
            writer.write(tab(1) + "public static enum " + encodingName + " implements Encoding{");
            writer.newLine();

            int i = 0;
            for (AmqpEncoding encoding : encodings) {
                i++;
                String eName = encoding.getName();
                if (eName == null) {
                    eName = name;
                }
                eName = toJavaConstant(eName);

                writer.write(tab(2) + eName + " (" + toJavaConstant(encoding.getName()) + "_FORMAT_CODE)");
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
            writer.write(tab(2) + "public final byte getEncodingFormatCode() {");
            writer.newLine();
            writer.write(tab(3) + "return FORMAT_CODE;");
            writer.newLine();
            writer.write(tab(2) + "}");
            writer.newLine();

            writer.newLine();
            writer.write(tab(2) + "public final AmqpVersion getEncodingVersion() {");
            writer.newLine();
            writer.write(tab(3) + "return AmqpMarshaller.VERSION;");
            writer.newLine();
            writer.write(tab(2) + "}");
            writer.newLine();

            writer.newLine();
            writer.write(tab(2) + "public static " + encodingName + " getEncoding(byte formatCode) throws UnexpectedTypeException {");
            writer.newLine();
            writer.write(tab(3) + "switch(formatCode) {");
            writer.newLine();
            for (AmqpEncoding encoding : encodings) {
                writer.write(tab(3) + "case " + toJavaConstant(encoding.getName()) + "_FORMAT_CODE: {");
                writer.newLine();
                writer.write(tab(4) + "return " + toJavaConstant(encoding.getName()) + ";");
                writer.newLine();
                writer.write(tab(3) + "}");
                writer.newLine();
            }
            writer.write(tab(3) + "default: {");
            writer.newLine();
            writer.write(tab(4) + "throw new UnexpectedTypeException(\"Unexpected format code for " + capFirst(name) + ": \" + formatCode);");
            writer.newLine();
            writer.write(tab(3) + "}");
            writer.newLine();
            writer.write(tab(3) + "}");
            writer.newLine();
            writer.write(tab(2) + "}");
            writer.newLine();

            writer.newLine();
            writer.write(tab(2) + "static final " + getJavaType() + "Encoded createEncoded(EncodedBuffer buffer) throws AmqpEncodingError {");
            writer.newLine();
            writer.write(tab(3) + "switch(buffer.getEncodingFormatCode()) {");
            writer.newLine();
            for (AmqpEncoding encoding : encodings) {
                writer.write(tab(3) + "case " + toJavaConstant(encoding.getName()) + "_FORMAT_CODE: {");
                writer.newLine();
                writer.write(tab(4) + "return new " + getJavaType() + capFirst(toJavaName(encoding.getName())) + "Encoded(buffer);");
                writer.newLine();
                writer.write(tab(3) + "}");
                writer.newLine();
            }
            writer.write(tab(3) + "default: {");
            writer.newLine();
            writer.write(tab(4) + "throw new UnexpectedTypeException(\"Unexpected format code for " + capFirst(name) + ": \" + buffer.getEncodingFormatCode());");
            writer.newLine();
            writer.write(tab(3) + "}");
            writer.newLine();
            writer.write(tab(3) + "}");
            writer.newLine();
            writer.write(tab(2) + "}");
            writer.newLine();

            writer.write(tab(2) + "static final " + getJavaType() + "Encoded createEncoded(byte formatCode, " + getValueMapping().getJavaType() + " value) throws AmqpEncodingError {");
            writer.newLine();
            writer.write(tab(3) + "switch(formatCode) {");
            writer.newLine();
            for (AmqpEncoding encoding : encodings) {
                writer.write(tab(3) + "case " + toJavaConstant(encoding.getName()) + "_FORMAT_CODE: {");
                writer.newLine();
                writer.write(tab(4) + "return new " + getJavaType() + capFirst(toJavaName(encoding.getName())) + "Encoded(value);");
                writer.newLine();
                writer.write(tab(3) + "}");
                writer.newLine();
            }
            writer.write(tab(3) + "default: {");
            writer.newLine();
            writer.write(tab(4) + "throw new UnexpectedTypeException(\"Unexpected format code for " + capFirst(name) + ": \" + formatCode);");
            writer.newLine();
            writer.write(tab(3) + "}");
            writer.newLine();
            writer.write(tab(3) + "}");
            writer.newLine();
            writer.write(tab(2) + "}");
            writer.newLine();
            

            writer.write(tab(1) + "}");
            writer.newLine();

            String decoderArg = "";
            writer.write(tab(1) + "public static abstract class " + getJavaType() + "Encoded extends AbstractEncoded <" + getValueMapping().getJavaType() + "> implements " + beanMapping + " {");
            if (isList()) {
                writer.newLine();
                writer.write(tab(2) + "ListDecoder decoder = Encoder.DEFAULT_LIST_DECODER;");
                writer.newLine();
                decoderArg = ", decoder";
            }

            if (isMap()) {
                writer.newLine();
                writer.write(tab(2) + "MapDecoder decoder = Encoder.DEFAULT_MAP_DECODER;");
                writer.newLine();
                decoderArg = ", decoder";
            }

            writer.newLine();
            writer.write(tab(2) + "public " + getJavaType() + "Encoded(EncodedBuffer encoded) {");
            writer.newLine();
            writer.write(tab(3) + "super(encoded);");
            writer.newLine();
            writer.write(tab(2) + "}");
            writer.newLine();

            writer.newLine();
            writer.write(tab(2) + "public " + getJavaType() + "Encoded(byte formatCode, " + getValueMapping().getJavaType() + " value) throws AmqpEncodingError {");
            writer.newLine();
            writer.write(tab(3) + "super(formatCode, value);");
            writer.newLine();
            writer.write(tab(2) + "}");
            writer.newLine();

            if (isList()) {
                writer.newLine();
                writer.write(tab(2) + "final void setDecoder(ListDecoder decoder) {");
                writer.newLine();
                writer.write(tab(3) + "this.decoder = decoder;");
                writer.newLine();
                writer.write(tab(2) + "}");
                writer.newLine();
            }

            if (isMap()) {
                writer.newLine();
                writer.write(tab(2) + "final void setDecoder(MapDecoder decoder) {");
                writer.newLine();
                writer.write(tab(3) + "this.decoder = decoder;");
                writer.newLine();
                writer.write(tab(2) + "}");
                writer.newLine();
            }
            
            if (isMap() || isList()) {
                writeFieldAccesors(writer, 2, true);
            }

            writer.write(tab(1) + "}");
            writer.newLine();

            for (AmqpEncoding encoding : encodings) {
                String eName = capFirst(toJavaName(encoding.getName()));

                writer.newLine();
                writeJavaComment(writer, 1, encoding.getLabel());
                writer.write(tab(1) + "private static class " + getJavaType() + eName + "Encoded extends " + getJavaType() + "Encoded {");
                writer.newLine();
                writer.newLine();
                writer.write(tab(2) + "private final " + encodingName + " encoding = " + encodingName + "." + toJavaConstant(encoding.getName()) + ";");

                writer.newLine();
                writer.write(tab(2) + "public " + getJavaType() + eName + "Encoded(EncodedBuffer encoded) {");
                writer.newLine();
                writer.write(tab(3) + "super(encoded);");
                writer.newLine();
                writer.write(tab(2) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(2) + "public " + getJavaType() + eName + "Encoded(" + getValueMapping().getJavaType() + " value) throws AmqpEncodingError {");
                writer.newLine();
                writer.write(tab(3) + "super(" + encodingName + "." + toJavaConstant(encoding.getName()) + ".FORMAT_CODE, value);");
                writer.newLine();
                writer.write(tab(2) + "}");
                writer.newLine();

                if (hasNonZeroEncoding()) {
                    writer.newLine();
                    writer.write(tab(2) + "protected final int computeDataSize() throws AmqpEncodingError {");
                    writer.newLine();
                    writer.write(tab(3) + "return ENCODER.getEncodedSizeOf" + capFirst(getName() + "(value, encoding);"));
                    writer.newLine();
                    writer.write(tab(2) + "}");
                    writer.newLine();
                }

                if (hasCompoundEncoding()) {
                    writer.newLine();
                    writer.write(tab(2) + "protected final int computeDataCount() throws AmqpEncodingError {");
                    writer.newLine();
                    writer.write(tab(3) + "return ENCODER.getEncodedCountOf" + capFirst(getName()) + "(value, encoding);");
                    writer.newLine();
                    writer.write(tab(2) + "}");
                    writer.newLine();
                }

                writer.newLine();
                writer.write(tab(2) + "public final void encode(" + getValueMapping().getJavaType() + " value, Buffer encoded, int offset) throws AmqpEncodingError {");
                writer.newLine();
                if (hasNonZeroEncoding()) {
                    writer.write(tab(3) + "ENCODER.encode" + capFirst(toJavaName(name)) + eName + "(value, encoded, offset);");
                }
                writer.newLine();
                writer.write(tab(2) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(2) + "public final " + getValueMapping().getJavaType() + " decode(EncodedBuffer encoded) throws AmqpEncodingError {");
                writer.newLine();
                if (hasCompoundEncoding()) {
                    writer.write(tab(3) + "return ENCODER.decode" + capFirst(toJavaName(name)) + eName + "(encoded.getBuffer(), encoded.getDataOffset(), encoded.getDataCount(), encoded.getDataSize()"
                            + decoderArg + ");");
                } else if (hasNonZeroEncoding()) {
                    writer.write(tab(3) + "return ENCODER.decode" + capFirst(toJavaName(name)) + eName + "(encoded.getBuffer(), encoded.getDataOffset(), encoded.getDataSize()" + decoderArg + ");");
                } else {
                    writer.write(tab(3) + "return ENCODER.valueOf" + capFirst(toJavaName(name)) + "(encoding);");
                }
                writer.newLine();
                writer.write(tab(2) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(2) + "public final void marshalData(DataOutput out) throws IOException {");
                writer.newLine();
                if (hasNonZeroEncoding()) {
                    writer.write(tab(3) + "ENCODER.write" + capFirst(toJavaName(name)) + eName + "(value, out);");
                }
                writer.newLine();
                writer.write(tab(2) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(2) + "public final " + getValueMapping().getJavaType() + " unmarshalData(DataInput in) throws IOException {");
                writer.newLine();

                if (hasCompoundEncoding()) {
                    writer.write(tab(3) + "return ENCODER.read" + capFirst(toJavaName(name)) + eName + "(getDataCount(), getDataSize(), in" + decoderArg + ");");
                } else if (hasNonZeroEncoding()) {
                    writer.write(tab(3) + "return ENCODER.read" + capFirst(toJavaName(name)) + eName + "(getDataSize(), in" + decoderArg + ");");
                } else {
                    writer.write(tab(3) + "return ENCODER.valueOf" + capFirst(toJavaName(name)) + "(encoding" + decoderArg + ");");
                }
                writer.newLine();
                writer.write(tab(2) + "}");
                writer.newLine();

                writer.write(tab(1) + "}");
                writer.newLine();
            }
        }
    }

    private boolean writeFields(BufferedWriter writer, int indent) throws IOException, UnknownTypeException {
        boolean ret = false;

        for (AmqpField field : fields.values()) {
            ret = true;
            JavaTypeMapping valueType = field.resolveAmqpFieldType().getValueMapping();

            writer.write(tab(indent) + "private " + valueType.getJavaType() + " " + field.getJavaName());

            if (field.getDefaultValue() != null) {
                writer.write(" = " + field.getDefaultValue());
            }

            writer.write(";");
            writer.newLine();
        }

        if (isPrimitive()) {
            writer.write(tab(indent) + "private " + getValueMapping().getJavaType() + " value;");
            writer.newLine();
        }
        return ret;
    }

    private void writeBeanImpl(BufferedWriter writer, int indent) throws IOException, UnknownTypeException {

        if (isPrimitive() || isDescribed()) {

            writer.newLine();
            writer.write(tab(indent) + "private " + beanMapping + " bean = this;");
            writer.newLine();
            writeFields(writer, indent);

            // CONSTRUCTORS:
            writer.newLine();
            writer.write(tab(indent) + "public " + typeMapping + "() {");
            writer.newLine();
            writer.write(tab(indent) + "}");
            writer.newLine();

            if (isPrimitive()) {

                writer.newLine();
                writer.write(tab(indent) + "public " + typeMapping + "(" + getValueMapping() + " value) {");
                writer.newLine();
                writer.write(tab(++indent) + "this.value = value;");
                writer.newLine();
                writer.write(tab(--indent) + "}");
                writer.newLine();
            }

            writer.newLine();
            writer.write(tab(indent) + "public " + typeMapping + "(Encoded<" + getValueMapping() + "> encoded) {");
            writer.newLine();
            if (isPrimitive()) {
                writer.write(tab(++indent) + "this.value = encoded.getValue();");
            } else {
                writer.write(tab(++indent) + "this.bean = encoded.getValue();");
            }
            writer.newLine();
            writer.write(tab(--indent) + "}");
            writer.newLine();

            writer.newLine();
            writer.write(tab(indent) + "public " + typeMapping + "(" + beanMapping + " other) {");
            writer.newLine();
            writer.write(tab(++indent) + "this.bean = other;");
            writer.newLine();
            writer.write(tab(--indent) + "}");
            writer.newLine();

            // METHODS:
            if (isCommand()) {
                writer.newLine();
                writer.write(tab(indent) + "public final void handle(AmqpCommandHandler handler) throws Exception {");
                writer.newLine();
                writer.write(tab(++indent) + "handler.handle" + capFirst(toJavaName(name)) + "(this);");
                writer.newLine();
                writer.write(tab(--indent) + "}");
                writer.newLine();
            }

            writer.newLine();
            writer.write(tab(indent) + "protected final Encoded<" + getValueMapping() + "> encode(AmqpMarshaller marshaller) throws AmqpEncodingError{");
            writer.newLine();
            writer.write(tab(++indent) + "return marshaller.encode(this);");
            writer.newLine();
            writer.write(tab(--indent) + "}");
            writer.newLine();

            writeFieldAccesors(writer, indent, false);
        } else if (isRestricted()) {
            AmqpClass restrictedType = resolveRestrictedType();

            // CONSTRUCTORS:
            writer.newLine();
            writer.write(tab(indent) + "public " + typeMapping + "() {");
            writer.newLine();
            writer.write(tab(indent + 1) + "super();");
            writer.newLine();
            writer.write(tab(indent) + "}");
            writer.newLine();

            if (restrictedType.isPrimitive()) {

                writer.newLine();
                writer.write(tab(indent) + "public " + typeMapping + "(" + restrictedType.getValueMapping() + " value) {");
                writer.newLine();
                writer.write(tab(++indent) + "super(value);");
                writer.newLine();
                writer.write(tab(--indent) + "}");
                writer.newLine();
            }

            writer.newLine();
            writer.write(tab(indent) + "public " + typeMapping + "(Encoded<" + restrictedType.getValueMapping() + "> encoded) {");
            writer.newLine();
            writer.write(tab(++indent) + "super(encoded);");
            writer.newLine();
            writer.write(tab(--indent) + "}");
            writer.newLine();

            writer.newLine();
            writer.write(tab(indent) + "public " + typeMapping + "(" + restrictedType.getBeanMapping() + " other) {");
            writer.newLine();
            writer.write(tab(++indent) + "super(other);");
            writer.newLine();
            writer.write(tab(--indent) + "}");
            writer.newLine();
        }

    }

    private boolean writeFieldAccesors(BufferedWriter writer, int indent, boolean encoder) throws IOException, UnknownTypeException {
        boolean ret = false;

        for (AmqpField field : fields.values()) {
            ret = true;

            JavaTypeMapping valueType = field.resolveAmqpFieldType().getValueMapping();
            // Setter:
            writer.newLine();
            field.writeJavaDoc(writer, indent);
            writer.write(tab(indent) + "public final void set" + capFirst(field.getJavaName()) + "(" + valueType + " " + toJavaName(field.getName()) + ") {");
            writer.newLine();
            writer.write(tab(++indent) + "this." + field.getJavaName() + " = " + toJavaName(field.getName()) + ";");
            writer.newLine();
            writer.write(tab(--indent) + "}");
            writer.newLine();
            writer.newLine();
            // Getter:
            field.writeJavaDoc(writer, indent);
            writer.write(tab(indent) + "public final " + valueType + " get" + capFirst(field.getJavaName()) + "() {");
            writer.newLine();
            writer.write(tab(++indent) + "return " + field.getJavaName() + ";");
            writer.newLine();
            writer.write(tab(--indent) + "}");
            writer.newLine();
        }

        if (isMap()) {
            writer.write(tab(indent) + "public void put(AmqpType<?> key, AmqpType<?> value) {");
            writer.newLine();
            writer.write(tab(++indent) + "this.value.put(key, value);");
            writer.newLine();
            writer.write(tab(--indent) + "}");
            writer.newLine();

            writer.newLine();
            writer.write(tab(indent) + "public AmqpType<?> get(AmqpType<?> key) {");
            writer.newLine();
            writer.write(tab(++indent) + "return value.get(key);");
            writer.newLine();
            writer.write(tab(--indent) + "}");
            writer.newLine();

        } else if (isList()) {
            writer.write(tab(indent) + "public void set(int index, AmqpType<?> value) {");
            writer.newLine();
            writer.write(tab(++indent) + "this.value.add(index, value);");
            writer.newLine();
            writer.write(tab(--indent) + "}");
            writer.newLine();

            writer.newLine();
            writer.write(tab(indent) + "public AmqpType<?> get(int index) {");
            writer.newLine();
            writer.write(tab(++indent) + "return value.get(index);");
            writer.newLine();
            writer.write(tab(--indent) + "}");
            writer.newLine();
        } 

        if (!encoder && isPrimitive()) {

            // Getter:
            writer.write(tab(indent) + "public final " + valueMapping + " getValue() {");
            writer.newLine();
            writer.write(tab(++indent) + "return value;");
            writer.newLine();
            writer.write(tab(--indent) + "}");
            writer.newLine();
        }
        return ret;
    }

    private void generateBeanInterface(Generator generator) throws IOException, UnknownTypeException {

        if (!needsMarshaller()) {
            return;
        }

        String packageName = beanMapping.getPackageName();

        File file = new File(generator.getOutputDirectory() + File.separator + new String(packageName).replace(".", File.separator) + File.separator + beanMapping + ".java");
        file.getParentFile().mkdirs();
        if (file.exists()) {
            file.delete();
        }
        BufferedWriter writer = new BufferedWriter(new FileWriter(file));

        writeJavaCopyWrite(writer);
        writer.write("package " + packageName + ";\n");
        writer.newLine();

        writeImports(writer, generator, true);

        writer.write("public interface " + beanMapping);
        // if (isDescribed()) {
        // writer.write(" extends " +
        // descriptor.resolveDescribedType().getBeanMapping());
        // }
        writer.write(" {");
        writer.newLine();

        if (isDescribed()) {
            writer.newLine();
            for (AmqpField field : fields.values()) {

                JavaTypeMapping valueType = field.resolveAmqpFieldType().getValueMapping();
                // Setter:
                writer.newLine();
                field.writeJavaDoc(writer, 1);
                writer.write(tab(1) + "public void set" + capFirst(field.getJavaName()) + "(" + valueType + " " + toJavaName(field.getName()) + ");");
                writer.newLine();

                // Getter:
                writer.newLine();
                field.writeJavaDoc(writer, 1);
                writer.write(tab(1) + "public " + valueType + " get" + capFirst(field.getJavaName()) + "();");
                writer.newLine();
            }
        }

        int indent = 1;
        if (isMap()) {
            writer.write(tab(indent) + "public void put(AmqpType<?> key, AmqpType<?> value);");
            writer.newLine();
            writer.newLine();
            writer.write(tab(indent) + "public AmqpType<?> get(AmqpType<?> key);");
            writer.newLine();

        } else if (isList()) {
            writer.write(tab(indent) + "public void set(int index, AmqpType<?> value);");
            writer.newLine();
            writer.newLine();
            writer.write(tab(indent) + "public AmqpType<?> get(int index);");
            writer.newLine();
        } 

        if (isPrimitive()) {
            // Getter:
            writer.newLine();
            writer.write(tab(1) + "public " + valueMapping + " getValue();");
            writer.newLine();
        }

        writer.newLine();
        writer.write("}");

        writer.flush();
        writer.close();

    }

    private void generateMarshaller(Generator generator) throws IOException, UnknownTypeException {
        if (!(isPrimitive() || descriptor != null)) {
            return;
        }

        String packageName = generator.getMarshallerPackage();

        File file = new File(generator.getOutputDirectory() + File.separator + new String(packageName).replace(".", File.separator) + File.separator + typeMapping + "Marshaller.java");
        file.getParentFile().mkdirs();
        if (file.exists()) {
            file.delete();
        }
        BufferedWriter writer = new BufferedWriter(new FileWriter(file));

        writeJavaCopyWrite(writer);
        writer.write("package " + packageName + ";\n");
        writer.newLine();
        if (writeImports(writer, generator, true)) {
            writer.newLine();
        }

        // Write out the descriptor (for compound types):
        if (descriptor != null) {
            writer.write("public class " + typeMapping.getShortName() + "Marshaller implements DescribedTypeMarshaller<" + typeMapping + ">{");
            writer.newLine();

            writer.newLine();
            writer.write(tab(1) + "static final " + typeMapping + "Marshaller SINGLETON = new " + typeMapping + "Marshaller();");
            writer.newLine();
            writer.write(tab(1) + "private static final Encoder ENCODER = Encoder.SINGLETON;");
            writer.newLine();

            writer.newLine();
            writer.write(tab(1) + "public static final String SYMBOLIC_ID = \"" + descriptor.getSymbolicName() + "\";");
            writer.newLine();
            writer.write(tab(1) + "//Format code: " + descriptor.getFormatCode() + ":");
            writer.newLine();
            writer.write(tab(1) + "public static final long CATEGORY = " + descriptor.getCategory() + ";");
            writer.newLine();
            writer.write(tab(1) + "public static final long DESCRIPTOR_ID = " + descriptor.getDescriptorId() + ";");
            writer.newLine();
            writer.write(tab(1) + "public static final long NUMERIC_ID = CATEGORY << 32 | DESCRIPTOR_ID; //(" + (descriptor.getCategory() << 32 | descriptor.getDescriptorId()) + "L)");
            writer.newLine();
            writer.write(tab(1) + "//Hard coded descriptor:");
            writer.newLine();
            writer.write(tab(1) + "public static final byte [] DESCRIPTOR = new byte [] {");
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

            String describedType = descriptor.getDescribedType();
            String typeMarshaller = null;
            String typeDataStructure = null;

            if (describedType.equals("map")) {
                writer.newLine();
                writer.write(tab(1) + "//Accessor keys for field mapped fields:");
                writer.newLine();
                for (AmqpField field : fields.values()) {

                    writer.write(tab(1) + "private static final AmqpSymbol " + toJavaConstant(field.getName()) + "_KEY = " + " new AmqpSymbol(\"" + field.getName() + "\");");
                    writer.newLine();
                }
                writer.newLine();

                typeMarshaller = "AmqpMapMarshaller";
                typeDataStructure = "AmqpMap";
            } else if (describedType.equals("list")) {
                typeMarshaller = "AmqpListMarshaller";
                typeDataStructure = "AmqpList";
            } else {
                throw new UnknownTypeException("Support for " + descriptor.getDescribedType() + " as a described type isn't yet implemented");
            }

            writeEncodings(writer);

            writer.newLine();
            writer.write(tab(1) + "public static final Encoded<" + beanMapping + "> encode(" + typeMapping + " value) throws AmqpEncodingError {");
            writer.newLine();
            writer.write(tab(2) + "return new " + typeMapping.getJavaType() + "Encoded(value);");
            writer.newLine();
            writer.write(tab(1) + "}");
            writer.newLine();

            writer.newLine();
            writer.write(tab(1) + "public static final <T extends " + typeMapping + "> T decodeType(T value, EncodedBuffer buffer) throws AmqpEncodingError {");
            writer.newLine();
            writer.write(tab(2) + "byte fc = buffer.getEncodingFormatCode();");
            writer.newLine();
            writer.write(tab(2) + "if (fc == Encoder.NULL_FORMAT_CODE) {");
            writer.newLine();
            writer.write(tab(3) + "return null;");
            writer.newLine();
            writer.write(tab(2) + "}");
            writer.newLine();
            writer.newLine();
            writer.write(tab(2) + "DescribedBuffer db = buffer.asDescribed();");
            writer.newLine();
            writer.write(tab(2) + "AmqpType<?> descriptor = AmqpMarshaller.SINGLETON.decodeType(db.getDescriptor());");
            writer.newLine();
            writer.write(tab(2) + "if(!(descriptor instanceof AmqpLong && ((AmqpLong)descriptor).getValue().longValue() == NUMERIC_ID ||");
            writer.newLine();
            writer.write(tab(3) + "   descriptor instanceof AmqpSymbol && ((AmqpSymbol)descriptor).getValue().equals(SYMBOLIC_ID))) {");
            writer.newLine();
            writer.write(tab(3) + "throw new UnexpectedTypeException(\"descriptor mismatch: \" + descriptor);");
            writer.newLine();
            writer.write(tab(2) + "}");
            writer.newLine();
            writer.write(tab(2) + "value.setEncoded(new " + getJavaType() + "Encoded(db));");
            writer.newLine();
            writer.write(tab(2) + "return value;");
            writer.newLine();
            writer.write(tab(1) + "}");
            writer.newLine();

            writer.newLine();
            writer.write(tab(1) + "public static final <T extends " + typeMapping + "> T unmarshalType(T value, DataInput in) throws IOException {");
            writer.newLine();
            writer.write(tab(2) + "byte fc = in.readByte();");
            writer.newLine();
            writer.write(tab(2) + "if (fc == Encoder.NULL_FORMAT_CODE) {");
            writer.newLine();
            writer.write(tab(3) + "return null;");
            writer.newLine();
            writer.write(tab(2) + "}");
            writer.newLine();
            writer.newLine();
            writer.write(tab(2) + "if (fc != Encoder.DESCRIBED_FORMAT_CODE) {");
            writer.newLine();
            writer.write(tab(3) + "throw new UnexpectedTypeException(\"unexpected format code: \" + fc);");
            writer.newLine();
            writer.write(tab(2) + "}");
            writer.newLine();
            writer.newLine();
            writer.write(tab(2) + "DescribedBuffer db = new DescribedBuffer(fc, in);");
            writer.newLine();
            writer.write(tab(2) + "AmqpType<?> descriptor = AmqpMarshaller.SINGLETON.decodeType(db.getDescriptor());");
            writer.newLine();
            writer.write(tab(2) + "if(!(descriptor instanceof AmqpLong && ((AmqpLong)descriptor).getValue().longValue() == NUMERIC_ID ||");
            writer.newLine();
            writer.write(tab(3) + "   descriptor instanceof AmqpSymbol && ((AmqpSymbol)descriptor).getValue().equals(SYMBOLIC_ID))) {");
            writer.newLine();
            writer.write(tab(3) + "throw new UnexpectedTypeException(\"descriptor mismatch: \" + descriptor);");
            writer.newLine();
            writer.write(tab(2) + "}");
            writer.newLine();
            writer.write(tab(2) + "value.setEncoded(new " + getJavaType() + "Encoded(db));");
            writer.newLine();
            writer.write(tab(2) + "return value;");
            writer.newLine();
            writer.write(tab(1) + "}");
            writer.newLine();

            writer.newLine();
            writer.write(tab(1) + "public final " + typeMapping + " decodeDescribedType(AmqpType<?> descriptor, DescribedBuffer encoded) throws AmqpEncodingError {");
            writer.newLine();
            writer.write(tab(2) + getJavaType() + " rc = new " + getJavaType() + "();");
            writer.newLine();
            writer.write(tab(2) + "rc.setEncoded(new " + getJavaType() + "Encoded(encoded));");
            writer.newLine();
            writer.write(tab(2) + "return rc;");
            writer.newLine();
            writer.write(tab(1) + "}");
            writer.newLine();

        }

        // Add accessors for primitive encoded sizes:
        if (isPrimitive()) {

            writer.write("public class " + typeMapping.getShortName() + "Marshaller {");
            writer.newLine();

            writer.newLine();
            writer.write(tab(1) + "private static final Encoder ENCODER = Encoder.SINGLETON;");
            writer.newLine();

            writeEncodings(writer);

            writer.newLine();
            // Handle fixed width encodings:
            if (!hasMultipleEncodings() && !hasNonFixedEncoding()) {
                writer.newLine();
                writer.write(tab(1) + "public static final " + getJavaType() + "Encoded encode(" + getJavaType() + " data) throws AmqpEncodingError {");
                writer.newLine();
                writer.write(tab(2) + "return new " + getJavaType() + "Encoded(data.getValue());");
                writer.newLine();
                writer.write(tab(1) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(1) + "static final " + getJavaType() + "Encoded createEncoded(EncodedBuffer buffer) throws AmqpEncodingError {");
                writer.newLine();
                writer.write(tab(2) + "if(buffer.getEncodingFormatCode() == AmqpNullMarshaller.FORMAT_CODE) {");
                writer.newLine();
                writer.write(tab(3) + "return null;");
                writer.newLine();
                writer.write(tab(2) + "}");
                writer.newLine();
                writer.write(tab(2) + "if(buffer.getEncodingFormatCode() != FORMAT_CODE) {");
                writer.newLine();
                writer.write(tab(3) + "throw new AmqpEncodingError(\"Unexpected format for " + typeMapping.getShortName() + " expected: \" + FORMAT_CODE);");
                writer.newLine();
                writer.write(tab(2) + "}");
                writer.newLine();
                writer.write(tab(2) + "return new " + typeMapping.getJavaType() + "Encoded(buffer);");
                writer.newLine();
                writer.write(tab(1) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(1) + "public static final " + typeMapping.getShortName() + " decodeType(EncodedBuffer buffer) throws AmqpEncodingError {");
                writer.newLine();
                writer.write(tab(2) + "return decodeType(new " + typeMapping.getJavaType() + "(), buffer);");
                writer.newLine();
                writer.write(tab(1) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(1) + "public static final <T extends " + typeMapping.getShortName() + "> T decodeType(T value, EncodedBuffer buffer) throws AmqpEncodingError {");
                writer.newLine();
                writer.write(tab(2) + "AbstractEncoded<" + getValueMapping().getJavaType() + "> encoded = createEncoded(buffer);");
                writer.newLine();
                writer.write(tab(2) + "if(encoded == null) {");
                writer.newLine();
                writer.write(tab(3) + "return null;");
                writer.newLine();
                writer.write(tab(2) + "}");
                writer.newLine();
                writer.write(tab(2) + "value.setEncoded(encoded);");
                writer.newLine();
                writer.write(tab(2) + "return value;");
                writer.newLine();
                writer.write(tab(1) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(1) + "public static final " + typeMapping.getShortName() + " unmarshalType(DataInput in) throws IOException {");
                writer.newLine();
                writer.write(tab(2) + "return decodeType(new " + typeMapping.getShortName() + "(), FormatCategory.createBuffer(in.readByte(), in));");
                writer.newLine();
                writer.write(tab(1) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(1) + "public static final <T extends " + typeMapping.getShortName() + "> T unmarshalType(T value, DataInput in) throws IOException {");
                writer.newLine();
                writer.write(tab(2) + "return decodeType(value, FormatCategory.createBuffer(in.readByte(), in));");
                writer.newLine();
                writer.write(tab(1) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(1) + "public static final <T extends " + typeMapping.getShortName() + "> T  unmarshalData(T value, Encoding encoding, DataInput in) throws IOException {");
                writer.newLine();
                writer.write(tab(2) + "return decodeType(value, FormatCategory.createBuffer(encoding.getEncodingFormatCode(), in));");
                writer.newLine();
                writer.write(tab(1) + "}");
                writer.newLine();

            } else {

                writer.newLine();
                writer.write(tab(1) + "public static final " + getEncodingName(false) + " chooseEncoding(" + getJavaType() + " val) throws AmqpEncodingError {");
                writer.newLine();
                writer.write(tab(2) + "return Encoder.choose" + capFirst(name) + "Encoding(val.getValue());");
                writer.newLine();
                writer.write(tab(1) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(1) + "public static final " + getJavaType() + "Encoded encode(" + getJavaType() + " data) throws AmqpEncodingError {");
                writer.newLine();
                writer.write(tab(2) + "return " + getEncodingName(false) + ".createEncoded(chooseEncoding(data).FORMAT_CODE, data.getValue());");
                writer.newLine();
                writer.write(tab(1) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(1) + "static final  " + getJavaType() + "Encoded createEncoded(EncodedBuffer buffer) throws AmqpEncodingError {");
                writer.newLine();
                writer.write(tab(2) + "if(buffer.getEncodingFormatCode() == AmqpNullMarshaller.FORMAT_CODE) {");
                writer.newLine();
                writer.write(tab(3) + "return null;");
                writer.newLine();
                writer.write(tab(2) + "}");
                writer.newLine();
                writer.write(tab(2) + "return " + getEncodingName(false) + ".createEncoded(buffer);");
                writer.newLine();
                writer.write(tab(1) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(1) + "public static final " + typeMapping.getShortName() + " decodeType(EncodedBuffer buffer) throws AmqpEncodingError {");
                writer.newLine();
                writer.write(tab(2) + "return decodeType(new " + typeMapping.getJavaType() + "(), buffer);");
                writer.newLine();
                writer.write(tab(1) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(1) + "public static final <T extends " + typeMapping.getShortName() + "> T decodeType(T value, EncodedBuffer buffer) throws AmqpEncodingError {");
                writer.newLine();
                writer.write(tab(2) + "Encoded<" + getValueMapping().getJavaType() + "> encoded = createEncoded(buffer);");
                writer.newLine();
                writer.write(tab(2) + "if(encoded == null) {");
                writer.newLine();
                writer.write(tab(3) + "return null;");
                writer.newLine();
                writer.write(tab(2) + "}");
                writer.newLine();
                writer.write(tab(2) + "value.setEncoded(encoded);");
                writer.newLine();
                writer.write(tab(2) + "return value;");
                writer.newLine();
                writer.write(tab(1) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(1) + "public static final " + typeMapping.getShortName() + " unmarshalType(DataInput in) throws IOException {");
                writer.newLine();
                writer.write(tab(2) + "return decodeType(new " + typeMapping.getShortName() + "(), FormatCategory.createBuffer(in.readByte(), in));");
                writer.newLine();
                writer.write(tab(1) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(1) + "public static final <T extends " + typeMapping.getShortName() + "> T unmarshalType(T value, DataInput in) throws IOException {");
                writer.newLine();
                writer.write(tab(2) + "return decodeType(value, FormatCategory.createBuffer(in.readByte(), in));");
                writer.newLine();
                writer.write(tab(1) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(1) + "public static final <T extends " + typeMapping.getShortName() + "> T  unmarshalData(T value, Encoding encoding, DataInput in) throws IOException {");
                writer.newLine();
                writer.write(tab(2) + "return decodeType(value, FormatCategory.createBuffer(encoding.getEncodingFormatCode(), in));");
                writer.newLine();
                writer.write(tab(1) + "}");
                writer.newLine();
            }
        }

        writer.write("}");
        writer.newLine();
        writer.flush();
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

    public boolean isAny() {
        return name.equals("*");
    }

    public boolean isPrimitive() {
        return primitive;
    }

    public boolean isRestricted() {
        return restricted;
    }

    public boolean isCommand() {
        return isCommand;
    }

    public String getRestrictedType() {
        return restrictedType;
    }

    public boolean isDescribed() {
        return descriptor != null;
    }

    public boolean isEnumType() {
        return isRestricted() && choice != null;
    }

    public String getMarshaller() throws UnknownTypeException {
        if (isAny()) {
            return "AmqpMarshaller.SINGLETON";
        } else if (isRestricted()) {
            return resolveRestrictedType().getTypeMapping() + "Marshaller";
        } else {
            return getTypeMapping() + "Marshaller";
        }
    }

    public boolean isMap() {
        return name.equals("map");
    }

    public boolean isList() {
        return name.equals("list");
    }

    public boolean isMarshallable() {
        return isDescribed() || isPrimitive() || (isRestricted() && !isEnumType());
    }

    public boolean needsMarshaller() {
        return isDescribed() || isPrimitive();
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
            return getJavaType() + "Marshaller." + getEncodingName(false);
        } else {
            return toJavaConstant(name) + "_ENCODING";
        }
    }

    public JavaTypeMapping getBeanMapping() {
        return beanMapping;
    }

    /**
     * Resolves the JavaTypeMapping that will be exposed via the class' api.
     * 
     * @return
     * @throws UnknownTypeException
     */
    public JavaTypeMapping getValueMapping() throws UnknownTypeException {
        return valueMapping;
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

    public String toString() {
        String ret = "Class: " + name + " [Encoding=" + encodings + ", Descriptor=" + descriptor + "]\n";
        ret += " Fields:\n";
        for (AmqpField f : fields.values()) {
            ret += " " + f.toString();
        }

        return ret;
    }
}
