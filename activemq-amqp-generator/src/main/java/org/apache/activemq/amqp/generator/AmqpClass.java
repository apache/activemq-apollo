package org.apache.activemq.amqp.generator;

import static org.apache.activemq.amqp.generator.Utils.capFirst;
import static org.apache.activemq.amqp.generator.Utils.padHex;
import static org.apache.activemq.amqp.generator.Utils.tab;
import static org.apache.activemq.amqp.generator.Utils.toJavaConstant;
import static org.apache.activemq.amqp.generator.Utils.toJavaName;
import static org.apache.activemq.amqp.generator.Utils.writeJavaComment;
import static org.apache.activemq.amqp.generator.Utils.writeJavaCopyWrite;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.rmi.UnexpectedException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
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
    // Java mapping of the bean for this type (if any)
    protected TypeRegistry.JavaTypeMapping beanMapping;
    // Java mapping of the buffer for this type (if any)
    protected TypeRegistry.JavaTypeMapping bufferMapping;

    protected String mapKeyType = "AmqpType<?,?>";
    protected String mapValueType = "AmqpType<?,?>";
    protected String listElementType = "AmqpType<?,?>";

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

        if (isMarshallable()) {
            beanMapping = new JavaTypeMapping(name + "-bean", generator.getPackagePrefix() + ".types", typeMapping + "." + typeMapping + "Bean", true);
            bufferMapping = new JavaTypeMapping(name + "-bean", generator.getPackagePrefix() + ".types", typeMapping + "." + typeMapping + "Buffer", true);
        }

        if (isPrimitive()) {
            valueMapping = TypeRegistry.getJavaTypeMapping(name);
        } else if (isRestricted()) {
            valueMapping = typeMapping;
        }
    }

    public void generate(Generator generator) throws IOException, UnknownTypeException {
        if (handcoded) {
            return;
        }

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
            writer.write("public enum " + typeMapping);
        } else if (isMarshallable()) {

            if (isRestricted()) {
                writer.write("public interface " + typeMapping + " extends " + resolveRestrictedType().getTypeMapping());
            } else if (isDescribed()) {
                writer.write("public interface " + typeMapping + " extends " + descriptor.resolveDescribedType().getTypeMapping());
            } else {
                writer.write("public interface " + typeMapping + " extends AmqpType<" + beanMapping + ", " + bufferMapping + ">");
            }
            
            if (isList() || isMap()) {
                writer.write(", " + valueMapping.getJavaType());
            }

            if (isCommand()) {
                writer.write(", AmqpCommand");
            }
        }

        writer.write(" {");
        writer.newLine();

        if (isMarshallable()) {
            writer.newLine();
            writeBeanInterface(writer, 1);
            writeBeanImpl(writer, 1);
            writeBufferImpl(writer, 1);
        } else {
            writeEnumType(writer);
        }

        writer.write("}");
        writer.flush();
        writer.close();

        // We don't generate beans or marshallers for restricted types:
        if (!isRestricted()) {
            generateMarshaller(generator);
        }

    }

    private boolean writeImports(BufferedWriter writer, Generator generator, boolean marshaller) throws IOException, UnknownTypeException {

        TreeSet<String> imports = new TreeSet<String>();
        for (AmqpField field : fields.values()) {

            AmqpClass fieldType = field.resolveAmqpFieldType();
            if (!marshaller) {
                filterOrAddImport(imports, fieldType.getValueMapping(), marshaller);
            }

            if (fieldType.isEnumType()) {
                if (!marshaller) {
                    filterOrAddImport(imports, fieldType.getTypeMapping(), marshaller);
                }
                filterOrAddImport(imports, fieldType.resolveBaseType().getTypeMapping(), marshaller);
            } else {
                filterOrAddImport(imports, fieldType.getTypeMapping(), marshaller);
            }
        }

        if (!marshaller && isCommand()) {
            imports.add(generator.getPackagePrefix() + ".AmqpCommandHandler");
            imports.add(generator.getPackagePrefix() + ".AmqpCommand");
        }

        if (marshaller) {
            // Add the marshalled type:
            filterOrAddImport(imports, typeMapping, marshaller);

            if (isMap() || isList()) {
                filterOrAddImport(imports, TypeRegistry.any().getTypeMapping(), marshaller);
            }

            imports.add("java.io.DataInput");
            imports.add("java.io.IOException");
            imports.add(generator.getPackagePrefix() + ".marshaller.AmqpEncodingError");
            imports.add(generator.getPackagePrefix() + ".marshaller.Encoded");
            imports.add(generator.getMarshallerPackage() + ".Encoder");
            imports.add(generator.getMarshallerPackage() + ".Encoder.*");
            imports.add("org.apache.activemq.util.buffer.Buffer");

            if (isDescribed()) {

                filterOrAddImport(imports, getValueMapping(), marshaller);
                imports.add(generator.getPackagePrefix() + ".marshaller.UnexpectedTypeException");
                AmqpClass describedType = descriptor.resolveDescribedType();
                if (describedType.getName().equals("list")) {
                    imports.add(TypeRegistry.resolveAmqpClass("list").getValueMapping().getImport());
                    imports.add(generator.getPackagePrefix() + ".types.AmqpType");
                } else if (describedType.getName().equals("map")) {
                    imports.add(TypeRegistry.resolveAmqpClass("map").getValueMapping().getImport());
                    imports.add(generator.getPackagePrefix() + ".types.AmqpType");
                    imports.add("java.util.HashMap");
                    // Import symbol which is used for the keys:
                    imports.add(TypeRegistry.resolveAmqpClass("symbol").getTypeMapping().getImport());
                }

                imports.add(generator.getPackagePrefix() + ".types.AmqpUlong");
                imports.add(generator.getPackagePrefix() + ".types.AmqpSymbol");
            } else {
                imports.add("java.io.DataOutput");
            }

            if (hasMultipleEncodings()) {
                imports.add(generator.getPackagePrefix() + ".marshaller.UnexpectedTypeException");
                imports.add(generator.getPackagePrefix() + ".marshaller.Encoding");
                imports.add(generator.getPackagePrefix() + ".marshaller.AmqpVersion");
            }

            if (isPrimitive()) {
                filterOrAddImport(imports, getValueMapping(), marshaller);
            }

        } else if (isMarshallable()) {

            imports.add(generator.getPackagePrefix() + ".marshaller.AmqpEncodingError");
            imports.add(generator.getPackagePrefix() + ".marshaller.AmqpMarshaller");
            imports.add(generator.getPackagePrefix() + ".marshaller.Encoded");
            imports.add("org.apache.activemq.util.buffer.Buffer");
            imports.add("java.io.IOException");
            imports.add("java.io.DataOutput");
            imports.add("java.io.DataInput");

            imports.add(getValueMapping().getImport());

            if (resolveBaseType().isList()) {
                imports.add("java.util.Iterator");
            }

            if (isList()) {
                imports.add("java.util.ArrayList");
            }

            if (resolveBaseType().isMap()) {
                imports.add("java.util.Iterator");
                imports.add("java.util.Map");
                imports.add("java.util.HashMap");
            }

            if (descriptor != null) {

                AmqpClass describedType = descriptor.resolveDescribedType();
                if (describedType.getName().equals("list")) {
                    imports.add(TypeRegistry.resolveAmqpClass("list").getValueMapping().getImport());
                } else if (describedType.getName().equals("map")) {
                    imports.add(TypeRegistry.resolveAmqpClass("map").getValueMapping().getImport());
                }

                filterOrAddImport(imports, describedType.getTypeMapping(), marshaller);

                // filterOrAddImport(imports,
                // describedType.resolveValueMapping());
            }

            if (isCommand()) {
                imports.add(generator.getPackagePrefix() + ".AmqpCommandHandler");
                imports.add(generator.getPackagePrefix() + ".AmqpCommand");
            }
        }

        if (!marshaller && isRestricted()) {
            if (isEnumType()) {
                imports.add(generator.getPackagePrefix() + ".marshaller.AmqpEncodingError");
                imports.add("java.util.HashMap");
            }
            imports.add(TypeRegistry.resolveAmqpClass(restrictedType).getTypeMapping().getImport());
            imports.add(resolveRestrictedType().getValueMapping().getImport());
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
            writer.write(tab(2) + "this.value = new " + amqpClass.beanMapping + "(value);");

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

            AmqpClass describedType = descriptor.resolveDescribedType();

            if (describedType.isList()) {
                writer.newLine();
                writer.write(tab(1) + "private static final ListDecoder<" + TypeRegistry.any().typeMapping + "> DECODER = new ListDecoder<" + TypeRegistry.any().typeMapping + ">() {");
                writer.newLine();
                writer
                        .write(tab(2) + "public final IAmqpList<" + TypeRegistry.any().typeMapping
                                + "> unmarshalType(int dataCount, int dataSize, DataInput in) throws AmqpEncodingError, IOException {");
                writer.newLine();
                writer.write(tab(3) + "if (dataCount > " + fields.size() + ") {");
                writer.newLine();
                writer.write(tab(4) + "throw new AmqpEncodingError(\"Too many fields for \" + SYMBOLIC_ID + \": \" + dataCount);");
                writer.newLine();
                writer.write(tab(3) + "}");

                writer.newLine();
                writer.write(tab(3) + "IAmqpList<" + TypeRegistry.any().typeMapping + "> rc = new IAmqpList.ArrayBackedList<" + TypeRegistry.any().typeMapping + ">(new "
                        + TypeRegistry.any().typeMapping + "[" + fields.size() + "]);");
                int f = 0;
                for (AmqpField field : fields.values()) {
                    AmqpClass fieldType = field.resolveAmqpFieldType();
                    writer.newLine();
                    writer.write(tab(3) + "//" + field.getName() + ":");
                    writer.newLine();
                    writer.write(tab(3) + "if(dataCount > 0) {");
                    writer.newLine();
                    if (fieldType.isAny()) {
                        writer.write(tab(4) + "rc.set(" + f + ", AmqpMarshaller.SINGLETON.unmarshalType(in));");
                    } else {
                        writer.write(tab(4) + "rc.set(" + f + ", " + fieldType.getBufferMapping() + ".create(" + fieldType.getMarshaller() + ".createEncoded(in)));");
                    }
                    writer.newLine();
                    writer.write(tab(4) + "dataCount--;");
                    writer.newLine();
                    writer.write(tab(3) + "}");
                    writer.newLine();
                    if (field.isRequired()) {
                        writer.write(tab(3) + "else {");
                        writer.newLine();
                        writer.write(tab(4) + "throw new AmqpEncodingError(\"Missing required field for \" + SYMBOLIC_ID + \": " + field.getName() + "\");");
                        writer.newLine();
                        writer.write(tab(3) + "}");
                        writer.newLine();
                    }
                    f++;
                }

                writer.write(tab(3) + "return rc;");
                writer.newLine();
                writer.write(tab(2) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(2) + "public IAmqpList<" + TypeRegistry.any().typeMapping + "> decode(EncodedBuffer[] constituents) {");
                writer.newLine();
                writer.write(tab(3) + "if (constituents.length > " + fields.size() + ") {");
                writer.newLine();
                writer.write(tab(4) + "throw new AmqpEncodingError(\"Too many fields for \" + SYMBOLIC_ID + \":\" + constituents.length);");
                writer.newLine();
                writer.write(tab(3) + "}");

                writer.newLine();
                writer.write(tab(3) + "int dataCount = constituents.length;");
                writer.newLine();
                writer.write(tab(3) + "IAmqpList<" + TypeRegistry.any().typeMapping + "> rc = new IAmqpList.ArrayBackedList<" + TypeRegistry.any().typeMapping + ">(new "
                        + TypeRegistry.any().typeMapping + "[" + fields.size() + "]);");
                f = 0;
                for (AmqpField field : fields.values()) {
                    AmqpClass fieldType = field.resolveAmqpFieldType();
                    writer.newLine();
                    writer.write(tab(3) + "//" + field.getName() + ":");
                    writer.newLine();
                    writer.write(tab(3) + "if(dataCount > 0) {");
                    writer.newLine();
                    if (fieldType.isAny()) {
                        writer.write(tab(4) + "rc.set(" + f + ", AmqpMarshaller.SINGLETON.decodeType(constituents[" + f + "]));");
                    } else {
                        writer.write(tab(4) + "rc.set(" + f + ", " + fieldType.getBufferMapping() + ".create(" + fieldType.getMarshaller() + ".createEncoded(constituents[" + f + "])));");
                    }
                    writer.newLine();
                    writer.write(tab(4) + "dataCount--;");
                    writer.newLine();
                    writer.write(tab(3) + "}");
                    writer.newLine();
                    if (field.isRequired()) {
                        writer.write(tab(3) + "else {");
                        writer.newLine();
                        writer.write(tab(4) + "throw new AmqpEncodingError(\"Missing required field for \" + SYMBOLIC_ID + \": " + field.getName() + "\");");
                        writer.newLine();
                        writer.write(tab(3) + "}");
                        writer.newLine();
                    }
                    f++;
                }
                writer.write(tab(3) + "return rc;");
                writer.newLine();
                writer.write(tab(2) + "}");
                writer.newLine();

                writer.write(tab(1) + "};");
                writer.newLine();
            } else if (describedType.isMap()) {

                writer.newLine();
                writer.write(tab(1) + "private static final MapDecoder<AmqpSymbol, " + TypeRegistry.any().typeMapping + "> DECODER = new MapDecoder<AmqpSymbol, " + TypeRegistry.any().typeMapping
                        + ">() {");
                writer.newLine();

                writer.newLine();
                writer.write(tab(2) + "public IAmqpMap<AmqpSymbol, AmqpType<?, ?>> decode(EncodedBuffer[] constituents) throws AmqpEncodingError {");
                writer.newLine();
                writer.write(tab(3) + "IAmqpMap<AmqpSymbol, AmqpType<?, ?>> rc = new IAmqpMap.AmqpWrapperMap<AmqpSymbol, AmqpType<?,?>>(new HashMap<AmqpSymbol, AmqpType<?,?>>());");
                writer.newLine();
                writer.write(tab(3) + "if (constituents.length % 2 != 0) {");
                writer.newLine();
                writer.write(tab(4) + "throw new AmqpEncodingError(\"Invalid number of compound constituents for \" + SYMBOLIC_ID + \": \" + constituents.length);");
                writer.newLine();
                writer.write(tab(3) + "}");
                writer.newLine();

                writer.write(tab(3) + "for (int i = 0; i < constituents.length; i += 2) {");
                writer.newLine();
                writer.write(tab(4) + "AmqpSymbol key = AmqpSymbol.AmqpSymbolBuffer.create(AmqpSymbolMarshaller.createEncoded(constituents[i]));");
                writer.newLine();
                writer.write(tab(4) + "if (key == null) {");
                writer.newLine();
                writer.write(tab(5) + "throw new AmqpEncodingError(\"Null Key for \" + SYMBOLIC_ID);");
                writer.newLine();
                writer.write(tab(4) + "}");
                writer.newLine();
                writer.newLine();
                int f = 0;
                for (AmqpField field : fields.values()) {
                    AmqpClass fieldType = field.resolveAmqpFieldType();
                    writer.write(tab(4) + (f > 0 ? "else " : "") + "if (key.equals(" + typeMapping + "." + toJavaConstant(field.getName()) + "_KEY)){");
                    writer.newLine();
                    if (fieldType.isAny()) {
                        writer.write(tab(5) + "rc.put(" + typeMapping + "." + toJavaConstant(field.getName()) + "_KEY, AmqpMarshaller.SINGLETON.decodeType(constituents[i + 1]));");
                    } else {
                        writer.write(tab(5) + "rc.put(" + typeMapping + "." + toJavaConstant(field.getName()) + "_KEY, " + fieldType.getBufferMapping() + ".create(" + fieldType.getMarshaller()
                                + ".createEncoded(constituents[i + 1])));");
                    }
                    writer.newLine();
                    writer.write(tab(4) + "}");
                    writer.newLine();
                }
                writer.write(tab(4) + "else {");
                writer.newLine();
                writer.write(tab(5) + "throw new UnexpectedTypeException(\"Invalid field key for \" + SYMBOLIC_ID + \" : \" + key);");
                writer.newLine();
                writer.write(tab(4) + "}");
                writer.newLine();
                writer.write(tab(3) + "}");
                writer.newLine();
                writer.write(tab(3) + "return rc;");
                writer.newLine();
                writer.write(tab(2) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(2) + "public IAmqpMap<AmqpSymbol, AmqpType<?, ?>> unmarshalType(int dataCount, int dataSize, DataInput in) throws IOException, AmqpEncodingError {");
                writer.newLine();
                writer.write(tab(3) + "IAmqpMap<AmqpSymbol, AmqpType<?, ?>> rc = new IAmqpMap.AmqpWrapperMap<AmqpSymbol, AmqpType<?,?>>(new HashMap<AmqpSymbol, AmqpType<?,?>>());");
                writer.newLine();
                writer.write(tab(3) + "if (dataCount % 2 != 0) {");
                writer.newLine();
                writer.write(tab(4) + "throw new AmqpEncodingError(\"Invalid number of compound constituents for \" + SYMBOLIC_ID + \": \" + dataCount);");
                writer.newLine();
                writer.write(tab(3) + "}");
                writer.newLine();

                writer.write(tab(3) + "for (int i = 0; i < dataCount; i += 2) {");
                writer.newLine();
                writer.write(tab(4) + "AmqpSymbol key = AmqpSymbol.AmqpSymbolBuffer.create(AmqpSymbolMarshaller.createEncoded(in));");
                writer.newLine();
                writer.write(tab(4) + "if (key == null) {");
                writer.newLine();
                writer.write(tab(5) + "throw new AmqpEncodingError(\"Null Key for \" + SYMBOLIC_ID);");
                writer.newLine();
                writer.write(tab(4) + "}");
                writer.newLine();
                writer.newLine();
                f = 0;
                for (AmqpField field : fields.values()) {
                    AmqpClass fieldType = field.resolveAmqpFieldType();
                    writer.write(tab(4) + (f > 0 ? "else " : "") + "if (key.equals(" + typeMapping + "." + toJavaConstant(field.getName()) + "_KEY)){");
                    writer.newLine();
                    if (fieldType.isAny()) {
                        writer.write(tab(5) + "rc.put(" + typeMapping + "." + toJavaConstant(field.getName()) + "_KEY, AmqpMarshaller.SINGLETON.decodeType(in));");
                    } else {
                        writer.write(tab(5) + "rc.put(" + typeMapping + "." + toJavaConstant(field.getName()) + "_KEY, " + fieldType.getBufferMapping() + ".create(" + fieldType.getMarshaller()
                                + ".createEncoded(in)));");
                    }
                    writer.newLine();
                    writer.write(tab(4) + "}");
                    writer.newLine();
                }
                writer.write(tab(4) + "else {");
                writer.newLine();
                writer.write(tab(5) + "throw new UnexpectedTypeException(\"Invalid field key for \" + SYMBOLIC_ID + \" : \" + key);");
                writer.newLine();
                writer.write(tab(4) + "}");
                writer.newLine();
                writer.write(tab(3) + "}");
                writer.newLine();
                writer.write(tab(3) + "return rc;");
                writer.newLine();
                writer.write(tab(2) + "}");
                writer.newLine();

                writer.write(tab(1) + "};");
                writer.newLine();
            } else {
                throw new UnexpectedException("Unsupported described type: " + descriptor.getDescribedType());
            }

            writer.newLine();
            writer.write(tab(1) + "public static class " + getJavaType() + "Encoded extends DescribedEncoded<" + getValueMapping() + "> {");
            writer.newLine();

            writer.newLine();
            writer.write(tab(2) + "public " + getJavaType() + "Encoded(DescribedBuffer buffer) {");
            writer.newLine();
            writer.write(tab(3) + "super(buffer);");
            writer.newLine();
            writer.write(tab(2) + "}");
            writer.newLine();

            writer.newLine();
            writer.write(tab(2) + "public " + getJavaType() + "Encoded(" + typeMapping + " value) {");
            writer.newLine();
            writer.write(tab(3) + "super(" + describedType.getMarshaller() + ".encode(value));");
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

            writer.newLine();
            writer.write(tab(2) + "protected final Encoded<" + getValueMapping() + "> decodeDescribed(EncodedBuffer encoded) throws AmqpEncodingError {");
            writer.newLine();
            writer.write(tab(3) + "return " + describedType.getMarshaller() + ".createEncoded(encoded, DECODER);");
            writer.newLine();
            writer.write(tab(2) + "}");
            writer.newLine();

            writer.newLine();
            writer.write(tab(2) + "protected final Encoded<" + getValueMapping() + "> unmarshalDescribed(DataInput in) throws IOException {");
            writer.newLine();
            writer.write(tab(3) + "return " + describedType.getMarshaller() + ".createEncoded(in, DECODER);");
            writer.newLine();
            writer.write(tab(2) + "}");
            writer.newLine();

            writer.newLine();
            writer.write(tab(2) + "protected final EncodedBuffer getDescriptor() {");
            writer.newLine();
            writer.write(tab(3) + "return DESCRIPTOR;");
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
            writer.write(tab(1) + "public static class " + getJavaType() + "Encoded  extends AbstractEncoded<" + getValueMapping().getJavaType() + "> {");
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

            writer.write(tab(1) + "public static abstract class " + getJavaType() + "Encoded extends AbstractEncoded <" + getValueMapping().getJavaType() + "> {");
            if (isList()) {
                writer.newLine();
                writer.write(tab(2) + "ListDecoder decoder = Encoder.DEFAULT_LIST_DECODER;");
                writer.newLine();
            }

            if (isMap()) {
                writer.newLine();
                writer.write(tab(2) + "MapDecoder decoder = Encoder.DEFAULT_MAP_DECODER;");
                writer.newLine();
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
                writer.write(tab(2) + "public final void marshalData(DataOutput out) throws IOException {");
                writer.newLine();
                if (hasNonZeroEncoding()) {
                    writer.write(tab(3) + "ENCODER.write" + capFirst(toJavaName(name)) + eName + "(value, out);");
                }
                writer.newLine();
                writer.write(tab(2) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(2) + "public final " + getValueMapping().getJavaType() + " decode(EncodedBuffer encoded) throws AmqpEncodingError {");
                writer.newLine();
                if (isList() || isMap()) {
                    writer.write(tab(3) + "return decoder.decode(encoded.asCompound().constituents());");
                } else if (hasCompoundEncoding()) {
                    writer.write(tab(3) + "return ENCODER.decode" + capFirst(toJavaName(name)) + eName
                            + "(encoded.getBuffer(), encoded.getDataOffset(), encoded.getDataCount(), encoded.getDataSize());");
                } else if (hasNonZeroEncoding()) {
                    writer.write(tab(3) + "return ENCODER.decode" + capFirst(toJavaName(name)) + eName + "(encoded.getBuffer(), encoded.getDataOffset(), encoded.getDataSize());");
                } else {
                    writer.write(tab(3) + "return ENCODER.valueOf" + capFirst(toJavaName(name)) + "(encoding);");
                }
                writer.newLine();
                writer.write(tab(2) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(2) + "public final " + getValueMapping().getJavaType() + " unmarshalData(DataInput in) throws IOException {");
                writer.newLine();

                if (isList() || isMap()) {
                    writer.write(tab(3) + "return decoder.unmarshalType(getDataCount(), getDataSize(), in);");
                } else if (hasCompoundEncoding()) {
                    writer.write(tab(3) + "return ENCODER.read" + capFirst(toJavaName(name)) + eName + "(getDataCount(), getDataSize(), in);");
                } else if (hasNonZeroEncoding()) {
                    writer.write(tab(3) + "return ENCODER.read" + capFirst(toJavaName(name)) + eName + "(getDataSize(), in);");
                } else {
                    writer.write(tab(3) + "return ENCODER.valueOf" + capFirst(toJavaName(name)) + "(encoding);");
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
            AmqpClass fieldClass = field.resolveAmqpFieldType();
            JavaTypeMapping valueType = fieldClass.getTypeMapping();

            writer.write(tab(indent) + "private " + valueType + " " + field.getJavaName());

            if (field.getDefaultValue() != null) {
                writer.write(" = " + field.getDefaultValue());
            }

            writer.write(";");
            writer.newLine();
        }

        if (isDescribed()) {
            AmqpClass describedType = descriptor.resolveDescribedType();
            if (!describedType.isList()) {
                describedType.writeFields(writer, indent);
            }
        }

        if (isRestricted()) {
            resolveRestrictedType().writeFields(writer, indent);
        }

        if (isPrimitive()) {
            writer.write(tab(indent) + "private " + getValueMapping().getJavaType() + " value;");
            writer.newLine();
        }
        return ret;
    }

    private void writeBeanImpl(BufferedWriter writer, int indent) throws IOException, UnknownTypeException {

        AmqpClass baseType = resolveBaseType();

        writer.newLine();
        writer.write(tab(indent++) + "public static class " + beanMapping.getShortName() + " implements " + typeMapping + "{");
        writer.newLine();

        writer.newLine();
        writer.write(tab(indent) + "private " + bufferMapping.getShortName() + " buffer;");
        writer.newLine();
        writer.write(tab(indent) + "private " + beanMapping.getShortName() + " bean = this;");
        writer.newLine();
        writeFields(writer, indent);

        // CONSTRUCTORS:
        // Allow creation of unitialized mutable types:
        if (isMutable()) {
            writer.newLine();
            writer.write(tab(indent) + "" + beanMapping.getShortName() + "() {");
            writer.newLine();
            if (baseType.isMap()) {
                writer.write(tab(indent + 1) + "this.value = new IAmqpMap.AmqpWrapperMap<" + getMapKeyType() + "," + getMapValueType() + ">(new HashMap<" + getMapKeyType() + "," + getMapValueType()
                        + ">());");
                writer.newLine();
            } else if (!isDescribed() && baseType.isList()) {
                writer.write(tab(indent + 1) + "this.value = new IAmqpList.AmqpWrapperList(new ArrayList<AmqpType<?,?>>());");
                writer.newLine();
            }
            writer.write(tab(indent) + "}");
            writer.newLine();
        }

        writer.newLine();
        writer.write(tab(indent) + beanMapping.getShortName() + "(" + baseType.getValueMapping().getJavaType() + " value) {");
        writer.newLine();
        if (isDescribed() && baseType.isList()) {
            writer.newLine();
            writer.write(tab(++indent) + "for(int i = 0; i < value.getListCount(); i++) {");
            writer.newLine();
            writer.write(tab(++indent) + "set(i, value.get(i));");
            writer.newLine();
            writer.write(tab(--indent) + "}");
        } else {
            writer.write(tab(++indent) + "this.value = value;");
        }
        writer.newLine();
        writer.write(tab(--indent) + "}");
        writer.newLine();

        writer.newLine();
        writer.write(tab(indent) + beanMapping.getShortName() + "(" + beanMapping + " other) {");
        writer.newLine();
        writer.write(tab(indent + 1) + "this.bean = other;");
        writer.newLine();

        writer.write(tab(indent) + "}");
        writer.newLine();

        // METHODS:
        writer.newLine();
        writer.write(tab(indent) + "public final " + beanMapping.getShortName() + " copy() {");
        writer.newLine();
        if (isMutable()) {
            writer.write(tab(++indent) + "return new " + beanMapping + "(bean);");
        } else {
            writer.write(tab(++indent) + "return bean;");
        }
        writer.newLine();
        writer.write(tab(--indent) + "}");
        writer.newLine();

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
        writer.write(tab(indent) + "public final " + bufferMapping + " getBuffer(AmqpMarshaller marshaller) throws AmqpEncodingError{");
        writer.newLine();
        writer.write(tab(++indent) + "if(buffer == null) {");
        writer.newLine();
        writer.write(tab(++indent) + "buffer = new " + bufferMapping.getShortName() + "(marshaller.encode(this));");
        writer.newLine();
        writer.write(tab(--indent) + "}");
        writer.newLine();
        writer.write(tab(indent) + "return buffer;");
        writer.newLine();
        writer.write(tab(--indent) + "}");
        writer.newLine();

        writer.newLine();
        writer.write(tab(indent) + "public final void marshal(DataOutput out, AmqpMarshaller marshaller) throws IOException, AmqpEncodingError{");
        writer.newLine();
        writer.write(tab(++indent) + "getBuffer(marshaller).marshal(out, marshaller);");
        writer.newLine();
        writer.write(tab(--indent) + "}");
        writer.newLine();

        // Accessors:
        writer.newLine();
        writeFieldAccesors(writer, indent, false);
        writer.newLine();

        if (isMutable()) {
            writer.newLine();
            writer.write(tab(indent) + "private final void copyCheck() {");
            writer.newLine();
            writer.write(tab(++indent) + "if(buffer != null) {;");
            writer.newLine();
            writer.write(tab(++indent) + "throw new IllegalStateException(\"unwriteable\");");
            writer.newLine();
            writer.write(tab(--indent) + "}");
            writer.newLine();
            writer.write(tab(indent) + "if(bean != this) {;");
            writer.newLine();
            writer.write(tab(++indent) + "copy(bean);");
            writer.newLine();
            writer.write(tab(--indent) + "}");
            writer.newLine();
            writer.write(tab(--indent) + "}");
            writer.newLine();

            writer.newLine();
            writer.write(tab(indent) + "private final void copy(" + beanMapping + " other) {");
            writer.newLine();
            indent++;

            if (isDescribed()) {
                for (AmqpField field : baseType.fields.values()) {
                    writer.write(tab(indent) + "this." + field.getJavaName() + "= other." + field.getJavaName() + ";");
                    writer.newLine();
                }
            } else {
                writer.write(tab(indent) + "this.value = other.value;");
                writer.newLine();
            }
            writer.write(tab(indent) + "bean = this;");
            writer.newLine();
            writer.write(tab(--indent) + "}");
            writer.newLine();
        }

        // Equivalency:
        writer.newLine();
        writer.write(tab(indent) + "public boolean equals(Object o){");
        writer.newLine();
        writer.write(tab(++indent) + "if(this == o) {");
        writer.newLine();
        writer.write(tab(++indent) + "return true;");
        writer.newLine();
        writer.write(tab(--indent) + "}");
        writer.newLine();
        writer.newLine();
        writer.write(tab(indent) + "if(o == null || !(o instanceof " + typeMapping + ")) {");
        writer.newLine();
        writer.write(tab(++indent) + "return false;");
        writer.newLine();
        writer.write(tab(--indent) + "}");
        writer.newLine();
        writer.newLine();
        writer.write(tab(indent) + "return equals((" + typeMapping + ") o);");
        writer.newLine();
        writer.write(tab(--indent) + "}");
        writer.newLine();

        writer.newLine();
        writer.write(tab(indent++) + "public boolean equals(" + typeMapping + " b) {");
        writer.newLine();
        if (isDescribed()) {
            for (AmqpField field : fields.values()) {

                writer.newLine();
                writer.write(tab(indent) + "if(b.get" + capFirst(field.getJavaName()) + "() == null ^ get" + capFirst(field.getJavaName()) + "() == null) {");
                writer.newLine();
                writer.write(tab(++indent) + "return false;");
                writer.newLine();
                writer.write(tab(--indent) + "}");
                writer.newLine();
                writer.write(tab(indent) + "if(b.get" + capFirst(field.getJavaName()) + "() != null && !b.get" + capFirst(field.getJavaName()) + "().equals(get" + capFirst(field.getJavaName())
                        + "())){ ");

                writer.newLine();
                writer.write(tab(++indent) + "return false;");
                writer.newLine();
                writer.write(tab(--indent) + "}");
                writer.newLine();
            }
            writer.write(tab(indent) + "return true;");
        } else if (baseType.isMap()) {
            writer.write(tab(indent) + "return AbstractAmqpMap.checkEqual(this, b);");
        } else if (baseType.isList()) {
            writer.write(tab(indent) + "return AbstractAmqpList.checkEqual(this, b);");
        } else {
            writer.write(tab(indent) + "if(b == null) {");
            writer.newLine();
            writer.write(tab(++indent) + "return false;");
            writer.newLine();
            writer.write(tab(--indent) + "}");
            writer.newLine();

            writer.newLine();
            writer.write(tab(indent) + "if(b.getValue() == null ^ getValue() == null) {");
            writer.newLine();
            writer.write(tab(++indent) + "return false;");
            writer.newLine();
            writer.write(tab(--indent) + "}");
            writer.newLine();
            writer.newLine();
            writer.write(tab(indent) + "return b.getValue() == null || b.getValue().equals(getValue());");
        }
        writer.newLine();
        writer.write(tab(--indent) + "}");
        writer.newLine();

        writer.newLine();
        writer.write(tab(indent) + "public int hashCode() {");
        writer.newLine();
        if (baseType.isMap()) {
            writer.write(tab(++indent) + "return AbstractAmqpMap.hashCodeFor(this);");
        } else if (baseType.isList()) {
            writer.write(tab(++indent) + "return AbstractAmqpList.hashCodeFor(this);");
        } else {
            writer.write(tab(++indent) + "if(getValue() == null) {");
            writer.newLine();
            writer.write(tab(++indent) + "return " + beanMapping + ".class.hashCode();");
            writer.newLine();
            writer.write(tab(--indent) + "}");
            writer.newLine();
            writer.write(tab(indent) + "return getValue().hashCode();");
        }
        writer.newLine();
        writer.write(tab(--indent) + "}");
        writer.newLine();

        writer.write(tab(--indent) + "}");
        writer.newLine();

    }

    private void writeBufferImpl(BufferedWriter writer, int indent) throws IOException, UnknownTypeException {
        if (isDescribed()) {
            AmqpClass describedType = descriptor.resolveDescribedType();

            writer.newLine();
            writer.write(tab(indent++) + "public static class " + bufferMapping.getClassName() + " extends " + describedType.bufferMapping + " implements " + typeMapping + "{");
            writer.newLine();

            writer.newLine();
            writer.write(tab(indent) + "private " + beanMapping.getShortName() + " bean;");
            writer.newLine();

            // CONSTRUCTORS:
            writer.newLine();
            writer.write(tab(indent) + "protected " + bufferMapping.getShortName() + "(Encoded<" + getValueMapping() + "> encoded) {");
            writer.newLine();
            writer.write(tab(++indent) + "super(encoded);");
            writer.newLine();
            writer.write(tab(--indent) + "}");
            writer.newLine();

        } else if (isPrimitive()) {

            writer.newLine();
            writer.write(tab(indent++) + "public static class " + bufferMapping.getShortName() + " implements " + typeMapping + ", AmqpBuffer< " + getValueMapping() + "> {");
            writer.newLine();

            writer.newLine();
            writer.write(tab(indent) + "private " + beanMapping.getShortName() + " bean;");
            writer.newLine();
            writer.write(tab(indent) + "protected Encoded<" + valueMapping + "> encoded;");
            writer.newLine();

            // CONSTRUCTORS:
            writer.newLine();
            writer.write(tab(indent) + "protected " + bufferMapping.getShortName() + "() {");
            writer.newLine();
            writer.write(tab(indent) + "}");
            writer.newLine();

            writer.newLine();
            writer.write(tab(indent) + "protected " + bufferMapping.getShortName() + "(Encoded<" + getValueMapping() + "> encoded) {");
            writer.newLine();
            writer.write(tab(++indent) + "this.encoded = encoded;");
            writer.newLine();
            writer.write(tab(--indent) + "}");
            writer.newLine();

            writer.newLine();
            writer.write(tab(indent) + "public final Encoded<" + getValueMapping() + "> getEncoded() throws AmqpEncodingError{");
            writer.newLine();
            writer.write(tab(++indent) + "return encoded;");
            writer.newLine();
            writer.write(tab(--indent) + "}");
            writer.newLine();

            writer.newLine();
            writer.write(tab(indent) + "public final void marshal(DataOutput out, AmqpMarshaller marshaller) throws IOException, AmqpEncodingError{");
            writer.newLine();
            writer.write(tab(++indent) + "encoded.marshal(out);");
            writer.newLine();
            writer.write(tab(--indent) + "}");
            writer.newLine();

        } else if (isRestricted()) {
            AmqpClass restrictedType = resolveRestrictedType();

            writer.newLine();
            writer.write(tab(indent++) + "public static class " + bufferMapping.getShortName() + " extends " + restrictedType.bufferMapping + " implements " + typeMapping + "{");
            writer.newLine();

            writer.newLine();
            writer.write(tab(indent) + "private " + beanMapping.getShortName() + " bean;");
            writer.newLine();

            // CONSTRUCTORS:
            writer.newLine();
            writer.write(tab(indent) + "protected " + bufferMapping.getShortName() + "() {");
            writer.newLine();
            writer.write(tab(++indent) + "super();");
            writer.newLine();
            writer.write(tab(--indent) + "}");
            writer.newLine();

            writer.newLine();
            writer.write(tab(indent) + "protected " + bufferMapping.getShortName() + "(Encoded<" + restrictedType.getValueMapping() + "> encoded) {");
            writer.newLine();
            writer.write(tab(++indent) + "super(encoded);");
            writer.newLine();
            writer.write(tab(--indent) + "}");
            writer.newLine();
        }

        // METHODS:
        writeFieldAccesors(writer, indent, true);

        writer.newLine();
        writer.write(tab(indent) + "public " + bufferMapping + " getBuffer(AmqpMarshaller marshaller) throws AmqpEncodingError{");
        writer.newLine();
        writer.write(tab(++indent) + "return this;");
        writer.newLine();
        writer.write(tab(--indent) + "}");
        writer.newLine();

        writer.newLine();
        writer.write(tab(indent) + "protected " + typeMapping + " bean() {");
        writer.newLine();
        writer.write(tab(++indent) + "if(bean == null) {");
        writer.newLine();
        writer.write(tab(++indent) + "bean = new " + beanMapping + "(encoded.getValue());");
        writer.newLine();
        writer.write(tab(indent) + "bean.buffer = this;");
        writer.newLine();
        writer.write(tab(--indent) + "}");
        writer.newLine();
        writer.write(tab(indent) + "return bean;");
        writer.newLine();
        writer.write(tab(--indent) + "}");
        writer.newLine();

        if (isCommand()) {
            writer.newLine();
            writer.write(tab(indent) + "public final void handle(AmqpCommandHandler handler) throws Exception {");
            writer.newLine();
            writer.write(tab(++indent) + "handler.handle" + capFirst(toJavaName(name)) + "(this);");
            writer.newLine();
            writer.write(tab(--indent) + "}");
            writer.newLine();
        }

        // Equivalency:

        writer.newLine();
        writer.write(tab(indent) + "public boolean equals(Object o){");
        writer.newLine();
        writer.write(tab(++indent) + "return bean().equals(o);");
        writer.newLine();
        writer.write(tab(--indent) + "}");
        writer.newLine();

        writer.newLine();
        writer.write(tab(indent) + "public boolean equals(" + typeMapping + " o){");
        writer.newLine();
        writer.write(tab(++indent) + "return bean().equals(o);");
        writer.newLine();
        writer.write(tab(--indent) + "}");
        writer.newLine();

        writer.newLine();
        writer.write(tab(indent) + "public int hashCode() {");
        writer.newLine();
        writer.write(tab(++indent) + "return bean().hashCode();");
        writer.newLine();
        writer.write(tab(--indent) + "}");
        writer.newLine();

        // Factory methods:
        writer.newLine();
        writer.write(tab(indent) + "public static " + bufferMapping + " create(Encoded<" + getEncodedType().valueMapping + "> encoded) {");
        writer.newLine();
        writer.write(tab(++indent) + "if(encoded.isNull()) {");
        writer.newLine();
        writer.write(tab(++indent) + "return null;");
        writer.newLine();
        writer.write(tab(--indent) + "}");
        writer.newLine();
        writer.write(tab(indent) + "return new " + bufferMapping + "(encoded);");
        writer.newLine();
        writer.write(tab(--indent) + "}");
        writer.newLine();

        writer.newLine();
        writer.write(tab(indent) + "public static " + bufferMapping + " create(DataInput in, AmqpMarshaller marshaller) throws IOException, AmqpEncodingError {");
        writer.newLine();
        writer.write(tab(++indent) + "return create(marshaller.unmarshal" + getEncodedType().typeMapping + "(in));");
        writer.newLine();
        writer.write(tab(--indent) + "}");
        writer.newLine();

        writer.newLine();
        writer.write(tab(indent) + "public static " + bufferMapping + " create(Buffer buffer, int offset, AmqpMarshaller marshaller) throws AmqpEncodingError {");
        writer.newLine();
        writer.write(tab(++indent) + "return create(marshaller.decode" + getEncodedType().typeMapping + "(buffer, offset));");
        writer.newLine();
        writer.write(tab(--indent) + "}");
        writer.newLine();

        writer.write(tab(--indent) + "}");
        writer.newLine();

    }

    private boolean writeFieldAccesors(BufferedWriter writer, int indent, boolean buffer) throws IOException, UnknownTypeException {
        boolean ret = false;

        for (AmqpField field : fields.values()) {
            ret = true;

            AmqpClass fieldClass = field.resolveAmqpFieldType();
            AmqpClass baseType = fieldClass.resolveBaseType();

            if (!buffer) {

                // Setters:
                if (baseType.isPrimitive() && !baseType.isAny() && !fieldClass.isEnumType() && !baseType.isDescribed() && !baseType.isMutable()) {
                    writer.newLine();
                    writer.write(tab(indent) + "public void set" + capFirst(field.getJavaName()) + "(" + baseType.valueMapping + " " + toJavaName(field.getName()) + ") {");
                    writer.newLine();
                    writer.write(tab(++indent) + "set" + capFirst(field.getJavaName()) + "(TypeFactory.create" + fieldClass.getJavaType() + "(" + toJavaName(field.getName()) + "));");
                    writer.newLine();
                    writer.write(tab(--indent) + "}");
                    writer.newLine();
                    writer.newLine();

                    if (baseType.getValueMapping().hasPrimitiveType()) {
                        writer.newLine();
                        writer.write(tab(indent) + "public void set" + capFirst(field.getJavaName()) + "(" + baseType.getValueMapping().getPrimitiveType() + " " + toJavaName(field.getName()) + ") {");
                        writer.newLine();
                        writer.write(tab(++indent) + "set" + capFirst(field.getJavaName()) + "(TypeFactory.create" + fieldClass.getJavaType() + "(" + toJavaName(field.getName()) + "));");
                        writer.newLine();
                        writer.write(tab(--indent) + "}");
                        writer.newLine();
                        writer.newLine();
                    }
                }

                writer.newLine();
                writer.write(tab(indent) + "public final void set" + capFirst(field.getJavaName()) + "(" + fieldClass.typeMapping + " " + toJavaName(field.getName()) + ") {");
                writer.newLine();
                writer.write(tab(++indent) + "copyCheck();");
                writer.newLine();
                writer.write(tab(indent) + "bean." + field.getJavaName() + " = " + toJavaName(field.getName()) + ";");
                writer.newLine();
                writer.write(tab(--indent) + "}");
                writer.newLine();
                writer.newLine();

                // Getter:
                JavaTypeMapping returnType = fieldClass.isPrimitive() ? fieldClass.getValueMapping() : fieldClass.typeMapping;
                writer.write(tab(indent) + "public final " + returnType + " get" + capFirst(field.getJavaName()) + "() {");
                writer.newLine();
                if (!fieldClass.isAny() && fieldClass.isPrimitive() && !fieldClass.isMutable()) {
                    writer.write(tab(++indent) + "return bean." + field.getJavaName() + ".getValue();");
                } else {
                    writer.write(tab(++indent) + "return bean." + field.getJavaName() + ";");
                }
                writer.newLine();
                writer.write(tab(--indent) + "}");
                writer.newLine();
            } else {

                // Setters:
                if (baseType.isPrimitive() && !baseType.isAny() && !fieldClass.isEnumType() && !baseType.isDescribed() && !baseType.isMutable()) {
                    writer.newLine();
                    writer.write(tab(indent) + "public void set" + capFirst(field.getJavaName()) + "(" + baseType.getValueMapping() + " " + toJavaName(field.getName()) + ") {");
                    writer.newLine();
                    writer.write(tab(++indent) + "bean().set" + capFirst(field.getJavaName()) + "(" + toJavaName(field.getName()) + ");");
                    writer.newLine();
                    writer.write(tab(--indent) + "}");
                    writer.newLine();

                    if (baseType.getValueMapping().hasPrimitiveType()) {
                        writer.newLine();
                        writer.write(tab(indent) + "public void set" + capFirst(field.getJavaName()) + "(" + baseType.getValueMapping().getPrimitiveType() + " " + toJavaName(field.getName()) + ") {");
                        writer.newLine();
                        writer.write(tab(++indent) + "bean().set" + capFirst(field.getJavaName()) + "(" + toJavaName(field.getName()) + ");");
                        writer.newLine();
                        writer.write(tab(--indent) + "}");
                        writer.newLine();
                        writer.newLine();
                    }
                }
                writer.newLine();
                writer.write(tab(indent) + "public final void set" + capFirst(field.getJavaName()) + "(" + fieldClass.getTypeMapping() + " " + toJavaName(field.getName()) + ") {");
                writer.newLine();
                writer.write(tab(++indent) + "bean().set" + capFirst(field.getJavaName()) + "(" + toJavaName(field.getName()) + ");");
                writer.newLine();
                writer.write(tab(--indent) + "}");
                writer.newLine();
                // Getter:
                JavaTypeMapping returnType = fieldClass.isPrimitive() ? fieldClass.getValueMapping() : fieldClass.typeMapping;
                writer.newLine();
                writer.write(tab(indent) + "public final " + returnType + " get" + capFirst(field.getJavaName()) + "() {");
                writer.newLine();
                writer.write(tab(++indent) + "return bean().get" + capFirst(field.getJavaName()) + "();");
                writer.newLine();
                writer.write(tab(--indent) + "}");
                writer.newLine();
            }
        }

        if (isMap()) {
            if (!buffer) {
                writer.write(tab(indent) + "public void put(" + getMapKeyType() + " key, " + getMapValueType() + " value) {");
                writer.newLine();
                writer.write(tab(++indent) + "copyCheck();");
                writer.newLine();
                writer.write(tab(indent) + "bean.value.put(key, value);");
                writer.newLine();
                writer.write(tab(--indent) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(indent) + "public " + getMapValueType() + " get(Object key) {");
                writer.newLine();
                writer.write(tab(++indent) + "return bean.value.get(key);");
                writer.newLine();
                writer.write(tab(--indent) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(indent) + "public int getEntryCount() {");
                writer.newLine();
                writer.write(tab(++indent) + "return bean.value.getEntryCount();");
                writer.newLine();
                writer.write(tab(--indent) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(indent) + "public Iterator<Map.Entry<" + getMapKeyType() + ", " + getMapValueType() + ">> iterator() {");
                writer.newLine();
                writer.write(tab(++indent) + "return bean.value.iterator();");
                writer.newLine();
                writer.write(tab(--indent) + "}");
                writer.newLine();

            } else {
                writer.write(tab(indent) + "public void put(" + getMapKeyType() + " key, " + getMapValueType() + " value) {");
                writer.newLine();
                writer.write(tab(++indent) + "bean().put(key, value);");
                writer.newLine();
                writer.write(tab(--indent) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(indent) + "public " + getMapValueType() + " get(Object key) {");
                writer.newLine();
                writer.write(tab(++indent) + "return bean().get(key);");
                writer.newLine();
                writer.write(tab(--indent) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(indent) + "public int getEntryCount() {");
                writer.newLine();
                writer.write(tab(++indent) + "return bean().getEntryCount();");
                writer.newLine();
                writer.write(tab(--indent) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(indent) + "public Iterator<Map.Entry<" + getMapKeyType() + ", " + getMapValueType() + ">> iterator() {");
                writer.newLine();
                writer.write(tab(++indent) + "return bean().iterator();");
                writer.newLine();
                writer.write(tab(--indent) + "}");
                writer.newLine();
            }

        } else if (isList()) {
            if (!buffer) {
                writer.newLine();
                writer.write(tab(indent) + "public void set(int index, " + TypeRegistry.any().typeMapping + " value) {");
                writer.newLine();
                writer.write(tab(++indent) + "copyCheck();");
                writer.newLine();
                writer.write(tab(indent) + "bean.value.set(index, value);");
                writer.newLine();
                writer.write(tab(--indent) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(indent) + "public " + TypeRegistry.any().typeMapping + " get(int index) {");
                writer.newLine();
                writer.write(tab(++indent) + "return bean.value.get(index);");
                writer.newLine();
                writer.write(tab(--indent) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(indent) + "public int getListCount() {");
                writer.newLine();
                writer.write(tab(++indent) + "return bean.value.getListCount();");
                writer.newLine();
                writer.write(tab(--indent) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(indent) + "public Iterator<" + TypeRegistry.any().typeMapping + "> iterator() {");
                writer.newLine();
                writer.write(tab(++indent) + "return new AmqpListIterator<" + TypeRegistry.any().typeMapping + ">(bean.value);");
                writer.newLine();
                writer.write(tab(--indent) + "}");
                writer.newLine();

            } else {
                writer.newLine();
                writer.write(tab(indent) + "public void set(int index, " + TypeRegistry.any().typeMapping + " value) {");
                writer.newLine();
                writer.write(tab(++indent) + "bean().set(index, value);");
                writer.newLine();
                writer.write(tab(--indent) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(indent) + "public " + TypeRegistry.any().typeMapping + " get(int index) {");
                writer.newLine();
                writer.write(tab(++indent) + "return bean().get(index);");
                writer.newLine();
                writer.write(tab(--indent) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(indent) + "public int getListCount() {");
                writer.newLine();
                writer.write(tab(++indent) + "return bean().getListCount();");
                writer.newLine();
                writer.write(tab(--indent) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(indent) + "public Iterator<" + TypeRegistry.any().typeMapping + "> iterator() {");
                writer.newLine();
                writer.write(tab(++indent) + "return bean().iterator();");
                writer.newLine();
                writer.write(tab(--indent) + "}");
                writer.newLine();
            }

        }

        if (isDescribed()) {
            if (descriptor.resolveDescribedType().isList() && !buffer) {

                writer.newLine();
                writer.write(tab(indent) + "public void set(int index, " + TypeRegistry.any().typeMapping + " value) {");
                writer.newLine();
                writer.write(tab(++indent) + "switch(index) {");
                writer.newLine();
                int f = 0;
                for (AmqpField field : fields.values()) {
                    writer.write(tab(indent) + "case " + f + ": {");
                    writer.newLine();
                    AmqpClass fieldClass = field.resolveAmqpFieldType();
                    if (fieldClass.isEnumType()) {
                        writer.write(tab(++indent) + "set" + capFirst(field.getJavaName()) + "(" + fieldClass.typeMapping + ".get((" + fieldClass.resolveRestrictedType().typeMapping + ")value));");
                    } else {
                        writer.write(tab(++indent) + "set" + capFirst(field.getJavaName()) + "((" + fieldClass.typeMapping + ") value);");
                    }
                    writer.newLine();
                    writer.write(tab(indent) + "break;");
                    writer.newLine();
                    writer.write(tab(--indent) + "}");
                    writer.newLine();
                    f++;
                }
                writer.write(tab(indent) + "default : {");
                writer.newLine();
                writer.write(tab(++indent) + "throw new IndexOutOfBoundsException(String.valueOf(index));");
                writer.newLine();
                writer.write(tab(--indent) + "}");
                writer.newLine();
                writer.write(tab(indent) + "}");
                writer.newLine();
                writer.write(tab(--indent) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(indent) + "public " + TypeRegistry.any().typeMapping + " get(int index) {");
                writer.newLine();
                writer.write(tab(++indent) + "switch(index) {");
                writer.newLine();
                f = 0;
                for (AmqpField field : fields.values()) {
                    writer.write(tab(indent) + "case " + f + ": {");
                    writer.newLine();
                    AmqpClass fieldClass = field.resolveAmqpFieldType();
                    if (fieldClass.isEnumType()) {
                        writer.write(tab(++indent) + "if(" + field.getJavaName() + " == null) {");
                        writer.newLine();
                        writer.write(tab(++indent) + "return null;");
                        writer.newLine();
                        writer.write(tab(--indent) + "}");
                        writer.newLine();
                        writer.write(tab(indent) + "return " + field.getJavaName() + ".getValue();");
                    } else {
                        writer.write(tab(++indent) + "return bean." + field.getJavaName() + ";");
                    }

                    writer.newLine();
                    writer.write(tab(--indent) + "}");
                    writer.newLine();
                    f++;
                }
                writer.write(tab(indent) + "default : {");
                writer.newLine();
                writer.write(tab(++indent) + "throw new IndexOutOfBoundsException(String.valueOf(index));");
                writer.newLine();
                writer.write(tab(--indent) + "}");
                writer.newLine();
                writer.write(tab(indent) + "}");
                writer.newLine();
                writer.write(tab(--indent) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(indent) + "public int getListCount() {");
                writer.newLine();
                writer.write(tab(++indent) + "return " + fields.size() + ";");
                writer.newLine();
                writer.write(tab(--indent) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(indent) + "public " + descriptor.resolveDescribedType().getValueMapping() + " getValue() {");
                writer.newLine();
                writer.write(tab(++indent) + "return bean;");
                writer.newLine();
                writer.write(tab(--indent) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(indent) + "public Iterator<AmqpType<?, ?>> iterator() {");
                writer.newLine();
                writer.write(tab(++indent) + "return new AmqpListIterator<" + TypeRegistry.any().typeMapping + ">(bean);");
                writer.newLine();
                writer.write(tab(--indent) + "}");
                writer.newLine();

            } else {
                descriptor.resolveDescribedType().writeFieldAccesors(writer, indent, buffer);
            }
        }

        if (isRestricted()) {
            resolveRestrictedType().writeFieldAccesors(writer, indent, buffer);
        }

        if (isPrimitive() && !isMutable()) {

            // Getter:
            writer.newLine();
            writer.write(tab(indent) + "public " + valueMapping + " getValue() {");
            writer.newLine();
            if (!buffer) {
                if (isList()) {
                    writer.write(tab(++indent) + "return bean;");
                } else {
                    writer.write(tab(++indent) + "return bean.value;");
                }
            } else {
                writer.write(tab(++indent) + "return bean().getValue();");
            }
            writer.newLine();
            writer.write(tab(--indent) + "}");
            writer.newLine();
        }
        return ret;
    }

    private void writeBeanInterface(BufferedWriter writer, int indent) throws IOException, UnknownTypeException {

        if (isDescribed()) {

            // Write out symbol constants
            if (resolveBaseType().isMap()) {
                writer.newLine();
                for (AmqpField field : fields.values()) {

                    AmqpClass symbolClass = TypeRegistry.resolveAmqpClass("symbol");
                    writeJavaComment(writer, indent, "Key for: " + field.getLabel());
                    writer.write(tab(1) + "public static final " + symbolClass.typeMapping + " " + toJavaConstant(field.getName()) + "_KEY = TypeFactory.create" + symbolClass.typeMapping + "(\""
                            + field.getName() + "\");");

                    writer.newLine();
                }
                writer.newLine();
            }

            writer.newLine();
            for (AmqpField field : fields.values()) {

                AmqpClass fieldClass = field.resolveAmqpFieldType();
                AmqpClass baseType = fieldClass.resolveBaseType();

                if (baseType.isPrimitive() && !baseType.isAny() && !fieldClass.isEnumType() && !baseType.isDescribed() && !baseType.isMutable()) {
                    // Setter:
                    writer.newLine();
                    field.getDoc().parseFromDoc(fieldClass.doc.docs);
                    field.writeJavaDoc(writer, indent);
                    writer.write(tab(indent) + "public void set" + capFirst(field.getJavaName()) + "(" + baseType.valueMapping + " " + toJavaName(field.getName()) + ");");
                    writer.newLine();

                    if (baseType.getValueMapping().hasPrimitiveType()) {
                        writer.newLine();
                        field.writeJavaDoc(writer, indent);
                        writer.write(tab(indent) + "public void set" + capFirst(field.getJavaName()) + "(" + baseType.getValueMapping().getPrimitiveType() + " " + toJavaName(field.getName()) + ");");
                        writer.newLine();
                    }

                }

                // Setters:
                writer.newLine();
                field.writeJavaDoc(writer, indent);
                writer.write(tab(1) + "public void set" + capFirst(field.getJavaName()) + "(" + fieldClass.typeMapping + " " + toJavaName(field.getName()) + ");");
                writer.newLine();

                // Getter:
                JavaTypeMapping returnType = fieldClass.isPrimitive() ? fieldClass.getValueMapping() : fieldClass.typeMapping;
                writer.newLine();
                field.writeJavaDoc(writer, indent);
                writer.write(tab(indent) + "public " + returnType + " get" + capFirst(field.getJavaName()) + "();");
                writer.newLine();
            }
        }

        if (isMap()) {
            doc.writeJavaDoc(writer, indent);
            writer.write(tab(indent) + "public void put(" + getMapKeyType() + " key, " + getMapValueType() + " value);");
            writer.newLine();
            writer.write(tab(indent) + "public " + getMapValueType() + " get(Object key);");
            writer.newLine();

        } else if (isList()) {
            doc.writeJavaDoc(writer, indent);
            writer.write(tab(indent) + "public void set(int index, " + TypeRegistry.any().typeMapping + " value);");
            writer.newLine();
            writer.write(tab(indent) + "public " + TypeRegistry.any().typeMapping + " get(int index);");
            writer.newLine();
            writer.write(tab(indent) + "public int getListCount();");
            writer.newLine();
        }

        if (isPrimitive() && !isMutable()) {
            // Getter:
            writer.newLine();
            writer.write(tab(1) + "public " + valueMapping + " getValue();");
            writer.newLine();
        }
    }

    private void generateMarshaller(Generator generator) throws IOException, UnknownTypeException {
        if (!(isPrimitive() || descriptor != null)) {
            return;
        }

        String packageName = generator.getMarshallerPackage();

        File file = new File(generator.getOutputDirectory() + File.separator + new String(packageName).replace(".", File.separator) + File.separator + typeMapping.getClassName() + "Marshaller.java");
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
            writer.write(tab(1) + "private static final Encoded<" + getValueMapping() + "> NULL_ENCODED = new Encoder.NullEncoded<" + getValueMapping() + ">();");
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
            writer.write(tab(1) + "public static final EncodedBuffer DESCRIPTOR = FormatCategory.createBuffer(new Buffer(new byte [] {");
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
            writer.write(tab(1) + "}), 0);");
            writer.newLine();

            AmqpClass describedType = descriptor.resolveDescribedType();

            if (!(describedType.isMap() || describedType.isList())) {
                throw new UnknownTypeException("Support for " + descriptor.getDescribedType() + " as a described type isn't yet implemented");
            }

            writeEncodings(writer);

            writer.newLine();
            writer.write(tab(1) + "public static final Encoded<" + getValueMapping() + "> encode(" + typeMapping + " value) throws AmqpEncodingError {");
            writer.newLine();
            writer.write(tab(2) + "return new " + typeMapping.getJavaType() + "Encoded(value);");
            writer.newLine();
            writer.write(tab(1) + "}");
            writer.newLine();

            writer.newLine();
            writer.write(tab(1) + "static final Encoded<" + getValueMapping() + "> createEncoded(Buffer source, int offset) throws AmqpEncodingError {");
            writer.newLine();
            writer.write(tab(2) + "return createEncoded(FormatCategory.createBuffer(source, offset));");
            writer.newLine();
            writer.write(tab(1) + "}");
            writer.newLine();

            writer.newLine();
            writer.write(tab(1) + "static final Encoded<" + getValueMapping() + "> createEncoded(DataInput in) throws IOException, AmqpEncodingError {");
            writer.newLine();
            writer.write(tab(2) + "return createEncoded(FormatCategory.createBuffer(in.readByte(), in));");
            writer.newLine();
            writer.write(tab(1) + "}");
            writer.newLine();

            writer.newLine();
            writer.write(tab(1) + "static final Encoded<" + getValueMapping() + "> createEncoded(EncodedBuffer buffer) throws AmqpEncodingError {");
            writer.newLine();
            writer.write(tab(2) + "byte fc = buffer.getEncodingFormatCode();");
            writer.newLine();
            writer.write(tab(2) + "if (fc == Encoder.NULL_FORMAT_CODE) {");
            writer.newLine();
            writer.write(tab(3) + "return NULL_ENCODED;");
            writer.newLine();
            writer.write(tab(2) + "}");
            writer.newLine();
            writer.newLine();
            writer.write(tab(2) + "DescribedBuffer db = buffer.asDescribed();");
            writer.newLine();
            writer.write(tab(2) + TypeRegistry.any().typeMapping + " descriptor = AmqpMarshaller.SINGLETON.decodeType(db.getDescriptorBuffer());");
            writer.newLine();
            writer.write(tab(2) + "if(!(descriptor instanceof AmqpUlong && ((AmqpUlong)descriptor).getValue().longValue() == NUMERIC_ID ||");
            writer.newLine();
            writer.write(tab(3) + "   descriptor instanceof AmqpSymbol && ((AmqpSymbol)descriptor).getValue().equals(SYMBOLIC_ID))) {");
            writer.newLine();
            writer.write(tab(3) + "throw new UnexpectedTypeException(\"descriptor mismatch: \" + descriptor);");
            writer.newLine();
            writer.write(tab(2) + "}");
            writer.newLine();
            writer.write(tab(2) + "return new " + getJavaType() + "Encoded(db);");
            writer.newLine();
            writer.write(tab(1) + "}");
            writer.newLine();

            writer.newLine();
            writer.write(tab(1) + "public final " + bufferMapping + " decodeDescribedType(" + TypeRegistry.any().typeMapping + " descriptor, DescribedBuffer encoded) throws AmqpEncodingError {");
            writer.newLine();
            writer.write(tab(2) + "return " + bufferMapping + ".create(new " + getJavaType() + "Encoded(encoded));");
            writer.newLine();
            writer.write(tab(1) + "}");
            writer.newLine();

        }
        // Add accessors for primitive encoded sizes:
        else if (isPrimitive()) {

            writer.write("public class " + typeMapping.getShortName() + "Marshaller {");
            writer.newLine();

            writer.newLine();
            writer.write(tab(1) + "private static final Encoder ENCODER = Encoder.SINGLETON;");
            writer.newLine();
            writer.write(tab(1) + "private static final Encoded<" + getValueMapping() + "> NULL_ENCODED = new Encoder.NullEncoded<" + getValueMapping() + ">();");

            writer.newLine();

            writeEncodings(writer);

            writer.newLine();
            // Handle fixed width encodings:
            if (!hasMultipleEncodings() && !hasNonFixedEncoding()) {
                writer.newLine();
                writer.write(tab(1) + "public static final Encoded<" + getValueMapping() + "> encode(" + getJavaType() + " data) throws AmqpEncodingError {");
                writer.newLine();
                writer.write(tab(2) + "if(data == null) {");
                writer.newLine();
                writer.write(tab(3) + "return NULL_ENCODED;");
                writer.newLine();
                writer.write(tab(2) + "}");
                writer.newLine();
                writer.write(tab(2) + "return new " + getJavaType() + "Encoded(data.getValue());");
                writer.newLine();
                writer.write(tab(1) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(1) + "static final Encoded<" + getValueMapping() + "> createEncoded(Buffer source, int offset) throws AmqpEncodingError {");
                writer.newLine();
                writer.write(tab(2) + "return createEncoded(FormatCategory.createBuffer(source, offset));");
                writer.newLine();
                writer.write(tab(1) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(1) + "static final Encoded<" + getValueMapping() + "> createEncoded(DataInput in) throws IOException, AmqpEncodingError {");
                writer.newLine();
                writer.write(tab(2) + "return createEncoded(FormatCategory.createBuffer(in.readByte(), in));");
                writer.newLine();
                writer.write(tab(1) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(1) + "static final Encoded<" + getValueMapping() + "> createEncoded(EncodedBuffer buffer) throws AmqpEncodingError {");
                writer.newLine();
                writer.write(tab(2) + "if(buffer.getEncodingFormatCode() == AmqpNullMarshaller.FORMAT_CODE) {");
                writer.newLine();
                writer.write(tab(3) + "return new Encoder.NullEncoded<" + getValueMapping() + ">();");
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

            } else {

                writer.newLine();
                writer.write(tab(1) + "private static final " + getEncodingName(false) + " chooseEncoding(" + getJavaType() + " val) throws AmqpEncodingError {");
                writer.newLine();
                if (isMutable()) {
                    writer.write(tab(2) + "return Encoder.choose" + capFirst(name) + "Encoding(val);");
                } else {
                    writer.write(tab(2) + "return Encoder.choose" + capFirst(name) + "Encoding(val.getValue());");
                }
                writer.newLine();
                writer.write(tab(1) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(1) + "private static final " + getEncodingName(false) + " chooseEncoding(" + valueMapping + " val) throws AmqpEncodingError {");
                writer.newLine();
                writer.write(tab(2) + "return Encoder.choose" + capFirst(name) + "Encoding(val);");
                writer.newLine();
                writer.write(tab(1) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(1) + "static final Encoded<" + getValueMapping() + "> encode(" + getJavaType() + " data) throws AmqpEncodingError {");
                writer.newLine();
                writer.write(tab(2) + "if(data == null) {");
                writer.newLine();
                writer.write(tab(3) + "return NULL_ENCODED;");
                writer.newLine();
                writer.write(tab(2) + "}");
                writer.newLine();
                if (isMutable()) {
                    writer.write(tab(2) + "return " + getEncodingName(false) + ".createEncoded(chooseEncoding(data).FORMAT_CODE, data);");
                } else {
                    writer.write(tab(2) + "return " + getEncodingName(false) + ".createEncoded(chooseEncoding(data).FORMAT_CODE, data.getValue());");
                }
                writer.newLine();
                writer.write(tab(1) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(1) + "static final Encoded<" + getValueMapping() + "> createEncoded(Buffer source, int offset) throws AmqpEncodingError {");
                writer.newLine();
                writer.write(tab(2) + "return createEncoded(FormatCategory.createBuffer(source, offset));");
                writer.newLine();
                writer.write(tab(1) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(1) + "static final Encoded<" + getValueMapping() + "> createEncoded(" + getValueMapping() + " val) throws AmqpEncodingError {");
                writer.newLine();
                writer.write(tab(2) + "return " + getEncodingName(false) + ".createEncoded(chooseEncoding(val).FORMAT_CODE, val);");
                writer.newLine();
                writer.write(tab(1) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(1) + "static final Encoded<" + getValueMapping() + "> createEncoded(DataInput in) throws IOException, AmqpEncodingError {");
                writer.newLine();
                writer.write(tab(2) + "return createEncoded(FormatCategory.createBuffer(in.readByte(), in));");
                writer.newLine();
                writer.write(tab(1) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(1) + "static final Encoded<" + getValueMapping() + "> createEncoded(EncodedBuffer buffer) throws AmqpEncodingError {");
                writer.newLine();
                writer.write(tab(2) + "if(buffer.getEncodingFormatCode() == AmqpNullMarshaller.FORMAT_CODE) {");
                writer.newLine();
                writer.write(tab(3) + "return NULL_ENCODED;");
                writer.newLine();
                writer.write(tab(2) + "}");
                writer.newLine();
                writer.write(tab(2) + "return " + getEncodingName(false) + ".createEncoded(buffer);");
                writer.newLine();
                writer.write(tab(1) + "}");
                writer.newLine();

                if (isList()) {
                    writer.newLine();
                    writer.write(tab(1) + "static final Encoded<" + getValueMapping() + "> createEncoded(DataInput in, ListDecoder decoder) throws IOException, AmqpEncodingError {");
                    writer.newLine();
                    writer.write(tab(2) + "return createEncoded(FormatCategory.createBuffer(in.readByte(), in), decoder);");
                    writer.newLine();
                    writer.write(tab(1) + "}");
                    writer.newLine();

                    writer.newLine();
                    writer.write(tab(1) + "static final Encoded<" + getValueMapping() + "> createEncoded(EncodedBuffer buffer, ListDecoder decoder) throws AmqpEncodingError {");
                    writer.newLine();
                    writer.write(tab(2) + "if(buffer.getEncodingFormatCode() == AmqpNullMarshaller.FORMAT_CODE) {");
                    writer.newLine();
                    writer.write(tab(3) + "return NULL_ENCODED;");
                    writer.newLine();
                    writer.write(tab(2) + "}");
                    writer.newLine();
                    writer.write(tab(2) + getJavaType() + "Encoded rc = " + getEncodingName(false) + ".createEncoded(buffer);");
                    writer.newLine();
                    writer.write(tab(2) + "rc.setDecoder(decoder);");
                    writer.newLine();
                    writer.write(tab(2) + "return rc;");
                    writer.newLine();
                    writer.write(tab(1) + "}");
                    writer.newLine();
                }

                if (isMap()) {
                    writer.newLine();
                    writer.write(tab(1) + "static final Encoded<" + getValueMapping() + "> createEncoded(DataInput in, MapDecoder decoder) throws IOException, AmqpEncodingError {");
                    writer.newLine();
                    writer.write(tab(2) + "return createEncoded(FormatCategory.createBuffer(in.readByte(), in), decoder);");
                    writer.newLine();
                    writer.write(tab(1) + "}");
                    writer.newLine();

                    writer.newLine();
                    writer.write(tab(1) + "static final Encoded<" + getValueMapping() + "> createEncoded(EncodedBuffer buffer, MapDecoder decoder) throws AmqpEncodingError {");
                    writer.newLine();
                    writer.write(tab(2) + "if(buffer.getEncodingFormatCode() == AmqpNullMarshaller.FORMAT_CODE) {");
                    writer.newLine();
                    writer.write(tab(3) + "return NULL_ENCODED;");
                    writer.newLine();
                    writer.write(tab(2) + "}");
                    writer.newLine();
                    writer.write(tab(2) + getJavaType() + "Encoded rc = " + getEncodingName(false) + ".createEncoded(buffer);");
                    writer.newLine();
                    writer.write(tab(2) + "rc.setDecoder(decoder);");
                    writer.newLine();
                    writer.write(tab(2) + "return rc;");
                    writer.newLine();
                    writer.write(tab(1) + "}");
                    writer.newLine();
                }
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

    public boolean isMutable() throws UnknownTypeException {
        AmqpClass baseType = resolveBaseType();
        if (baseType.isPrimitive() && !(baseType.isList() || baseType.isMap())) {
            return false;
        } else {
            return true;
        }
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

    public AmqpClass getEncodedType() throws UnknownTypeException {
        if (isAny()) {
            return TypeRegistry.any();
        } else if (isRestricted()) {
            return resolveRestrictedType();
        } else {
            return this;
        }
    }

    public AmqpClass getDescribedType() throws UnknownTypeException {
        return descriptor.resolveDescribedType();
    }

    public boolean isMap() {
        return name.equals("map");
    }

    public final String getMapKeyType() {
        return mapKeyType;
    }

    public final String getMapValueType() {
        return mapValueType;
    }

    public boolean isList() {
        return name.equals("list");
    }

    public final String getListElementType() {
        return listElementType;
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

    public JavaTypeMapping getBeanMapping() throws UnknownTypeException {
        if (beanMapping == null) {
            if (isEnumType()) {
                beanMapping = resolveRestrictedType().beanMapping;
            }
        }
        return beanMapping;
    }

    public JavaTypeMapping getBufferMapping() throws UnknownTypeException {
        if (bufferMapping == null) {
            if (isEnumType()) {
                bufferMapping = resolveRestrictedType().bufferMapping;
            }
        }
        return bufferMapping;
    }

    /**
     * Resolves the JavaTypeMapping that will be exposed via the class' api.
     * 
     * @return
     * @throws UnknownTypeException
     */
    public JavaTypeMapping getValueMapping() throws UnknownTypeException {
        if (valueMapping == null) {
            if (isDescribed()) {
                valueMapping = descriptor.resolveDescribedType().getValueMapping();
            }
        }

        return valueMapping;
    }

    /**
     * Resolves the TypeMapping of this class' base class
     * 
     * @return
     * @throws UnknownTypeException
     */
    public AmqpClass resolveBaseType() throws UnknownTypeException {
        if (isRestricted()) {
            return TypeRegistry.resolveAmqpClass(restrictedType);
        } else if (isDescribed()) {
            return getDescribedType();
        } else {
            return this;
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
