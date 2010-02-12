package org.apache.activemq.amqp.generator;

import static org.apache.activemq.amqp.generator.Utils.capFirst;
import static org.apache.activemq.amqp.generator.Utils.javaPackageOf;
import static org.apache.activemq.amqp.generator.Utils.tab;
import static org.apache.activemq.amqp.generator.Utils.toJavaConstant;
import static org.apache.activemq.amqp.generator.Utils.toJavaName;
import static org.apache.activemq.amqp.generator.Utils.writeJavaComment;
import static org.apache.activemq.amqp.generator.Utils.writeJavaCopyWrite;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.StringTokenizer;
import java.util.TreeSet;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.Source;
import javax.xml.transform.sax.SAXSource;

import org.apache.activemq.amqp.generator.TypeRegistry.JavaTypeMapping;
import org.apache.activemq.amqp.generator.jaxb.schema.Amqp;
import org.apache.activemq.amqp.generator.jaxb.schema.Definition;
import org.apache.activemq.amqp.generator.jaxb.schema.Section;
import org.apache.activemq.amqp.generator.jaxb.schema.Type;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;

public class Generator {

    private String[] inputFiles;
    private String outputDirectory;
    private String sourceDirectory;
    private String packagePrefix = "org.apache.activemq.amqp.protocol";

    public static final HashSet<String> CONTROLS = new HashSet<String>();
    public static final HashSet<String> COMMANDS = new HashSet<String>();
    public static final LinkedHashMap<String, AmqpDefinition> DEFINITIONS = new LinkedHashMap<String, AmqpDefinition>();

    public String[] getInputFile() {
        return inputFiles;
    }

    public void setInputFiles(String... inputFiles) {
        this.inputFiles = inputFiles;
    }

    public String getOutputDirectory() {
        return outputDirectory;
    }

    public void setOutputDirectory(String outputDirectory) {
        this.outputDirectory = outputDirectory;
    }

    public void setSourceDirectory(String sourceDirectory) {
        this.sourceDirectory = sourceDirectory;
    }

    public String getPackagePrefix() {
        return packagePrefix;
    }

    public void setPackagePrefix(String packagePrefix) {
        this.packagePrefix = packagePrefix;
    }

    public void generate() throws Exception {
        TypeRegistry.init(this);
        JAXBContext jc = JAXBContext.newInstance(Amqp.class.getPackage().getName());

        for (String inputFile : inputFiles) {

            // Firstly, scan the file for command and control defs:
            BufferedReader reader = new BufferedReader(new FileReader(new File(inputFile)));
            String line = reader.readLine();
            while (line != null) {
                line = line.trim();
                if (line.startsWith("<!-- -")) {
                    StringTokenizer tok = new StringTokenizer(line, "- ");
                    while (tok.hasMoreTokens()) {
                        String token = tok.nextToken();
                        if (token.equals("Control:")) {
                            CONTROLS.add(tok.nextToken());
                            break;
                        } else if (token.equals("Command:")) {
                            COMMANDS.add(tok.nextToken());
                            break;
                        }
                    }
                }
                line = reader.readLine();
            }
            reader.close();

            // JAXB has some namespace handling problems:
            Unmarshaller unmarshaller = jc.createUnmarshaller();
            SAXParserFactory parserFactory;
            parserFactory = SAXParserFactory.newInstance();
            parserFactory.setNamespaceAware(false);
            XMLReader xmlreader = parserFactory.newSAXParser().getXMLReader();
            Source er = new SAXSource(xmlreader, new InputSource(inputFile));
            // Amqp amqp = (Amqp) unmarshaller.unmarshal(new StreamSource(new
            // File(inputFile)), Amqp.class).getValue();
            Amqp amqp = (Amqp) unmarshaller.unmarshal(er);

            // Scan document:
            for (Object docOrSection : amqp.getDocOrSection()) {
                if (docOrSection instanceof Section) {
                    Section section = (Section) docOrSection;

                    for (Object docOrDefinitionOrType : section.getDocOrDefinitionOrType()) {
                        if (docOrDefinitionOrType instanceof Type) {
                            generateClassFromType(amqp, section, (Type) docOrDefinitionOrType);
                        } else if (docOrDefinitionOrType instanceof Definition) {
                            Definition def = (Definition) docOrDefinitionOrType;
                            DEFINITIONS.put(def.getName(), new AmqpDefinition(def));
                        }
                    }
                }
            }
        }

        // Copy handcoded:
        String handCodedSource = "org/apache/activemq/amqp/generator/handcoded";
        String outputPackage = packagePrefix.replace(".", File.separator);
        File sourceDir = new File(sourceDirectory + File.separator + handCodedSource);
        for (File javaFile : Utils.findFiles(sourceDir)) {
            if (!javaFile.getName().endsWith(".java")) {
                continue;
            }
            javaFile.setWritable(true);
            BufferedReader reader = new BufferedReader(new FileReader(javaFile));
            File out = new File(outputDirectory + File.separator + outputPackage + File.separator + javaFile.getCanonicalPath().substring((int) sourceDir.getCanonicalPath().length()));
            out.getParentFile().mkdirs();
            String line = reader.readLine();
            BufferedWriter writer = new BufferedWriter(new FileWriter(out));

            while (line != null) {
                line = line.replace("org.apache.activemq.amqp.generator.handcoded", packagePrefix);
                writer.write(line);
                writer.newLine();
                line = reader.readLine();
            }
            writer.flush();
            writer.close();
        }

        // Generate Types:
        for (AmqpClass amqpClass : TypeRegistry.getGeneratedTypes()) {
            amqpClass.generate(this);
        }

        generatePrimitiveEncoderInterface();
        generateCommandHandler();
        generateMarshallerInterface();
        generateMarshaller();
        generateTypeFactory();
        writeDefinitions();
    }

    private void generateCommandHandler() throws IOException {
        String outputPackage = packagePrefix.replace(".", File.separator);
        File out = new File(outputDirectory + File.separator + outputPackage + File.separator + "AmqpCommandHandler.java");

        BufferedWriter writer = new BufferedWriter(new FileWriter(out));

        writeJavaCopyWrite(writer);
        writer.write("package " + packagePrefix + ";");
        writer.newLine();
        writer.newLine();

        for (AmqpClass amqpClass : TypeRegistry.getGeneratedTypes()) {
            if (amqpClass.isCommand()) {
                writer.write("import " + amqpClass.getTypeMapping().getImport() + ";");
                writer.newLine();
            }
        }

        writer.write("public interface AmqpCommandHandler {");
        writer.newLine();
        // Generate Handler methods:
        for (AmqpClass amqpClass : TypeRegistry.getGeneratedTypes()) {
            if (amqpClass.isCommand()) {
                writer.newLine();
                writer.write(tab(1) + "public void handle" + capFirst(toJavaName(amqpClass.name)) + "(" + amqpClass.getJavaType() + " " + toJavaName(amqpClass.name) + ") throws Exception;");
                writer.newLine();
            }
        }
        writer.write("}");
        writer.flush();
        writer.close();
    }

    private void writeDefinitions() throws IOException {
        String outputPackage = packagePrefix.replace(".", File.separator);
        File out = new File(outputDirectory + File.separator + outputPackage + File.separator + "Definitions.java");

        BufferedWriter writer = new BufferedWriter(new FileWriter(out));

        writeJavaCopyWrite(writer);
        writer.write("package " + packagePrefix + ";");
        writer.newLine();
        writer.newLine();

        writer.write("public interface Definitions {");
        writer.newLine();
        // Generate Handler methods:
        for (AmqpDefinition def : DEFINITIONS.values()) {
            writer.newLine();
            def.writeJavaDoc(writer, 1);
            writer.write(tab(1) + "public static final String " + capFirst(toJavaConstant(def.getName())) + " = \"" + def.getValue() + "\";");
            writer.newLine();
        }

        writer.write("}");
        writer.flush();
        writer.close();
    }

    private void generateMarshallerInterface() throws IOException, UnknownTypeException {
        String outputPackage = new String(packagePrefix + ".marshaller");
        File out = new File(outputDirectory + File.separator + outputPackage.replace(".", File.separator) + File.separator + "AmqpMarshaller.java");

        BufferedWriter writer = new BufferedWriter(new FileWriter(out));

        writeJavaCopyWrite(writer);
        writer.write("package " + outputPackage + ";");
        writer.newLine();
        writer.newLine();

        TreeSet<String> imports = new TreeSet<String>();
        imports.add(getPackagePrefix() + ".marshaller.AmqpVersion");
        imports.add(getPackagePrefix() + ".marshaller.Encoded");
        writeMarshallerImports(writer, false, imports, outputPackage);

        writer.newLine();
        writer.write("public interface AmqpMarshaller {");
        writer.newLine();

        writer.newLine();
        writeJavaComment(writer, 1, "@return the protocol version of the marshaller");
        writer.write(tab(1) + "public AmqpVersion getVersion();");
        writer.newLine();

        writer.newLine();
        writer.write(tab(1) + "public " + TypeRegistry.any().typeMapping + " decodeType(Buffer source) throws AmqpEncodingError;");
        writer.newLine();

        writer.newLine();
        writer.write(tab(1) + "public " + TypeRegistry.any().typeMapping + " unmarshalType(DataInput in) throws IOException, AmqpEncodingError;");
        writer.newLine();

        // Generate Handler methods:
        for (AmqpClass amqpClass : TypeRegistry.getGeneratedTypes()) {
            if (amqpClass.needsMarshaller()) {

                if (amqpClass.name.equals("*")) {
                    continue;
                }

                writer.newLine();
                writer.write(tab(1) + "public Encoded<" + amqpClass.getValueMapping() + "> encode(" + amqpClass.getJavaType() + " data) throws AmqpEncodingError;");
                writer.newLine();

                writer.newLine();
                writer.write(tab(1) + "public Encoded<" + amqpClass.getValueMapping() + "> decode" + amqpClass.getJavaType() + "(Buffer source, int offset) throws AmqpEncodingError;");
                writer.newLine();

                writer.newLine();
                writer.write(tab(1) + "public Encoded<" + amqpClass.getValueMapping() + "> unmarshal" + amqpClass.getJavaType() + "(DataInput in) throws IOException, AmqpEncodingError;");
                writer.newLine();
            }
        }

        writer.write("}");
        writer.flush();
        writer.close();

    }

    private void generateMarshaller() throws IOException, UnknownTypeException {
        File out = new File(outputDirectory + File.separator + getMarshallerPackage().replace(".", File.separator) + File.separator + "AmqpMarshaller.java");

        BufferedWriter writer = new BufferedWriter(new FileWriter(out));

        writeJavaCopyWrite(writer);
        writer.write("package " + getMarshallerPackage() + ";");
        writer.newLine();
        writer.newLine();

        TreeSet<String> imports = new TreeSet<String>();
        imports.add("java.util.HashMap");
        imports.add(getMarshallerPackage() + ".Encoder.*");
        imports.add(getPackagePrefix() + ".marshaller.AmqpVersion");
        imports.add(getPackagePrefix() + ".marshaller.Encoded");
        writeMarshallerImports(writer, false, imports, getMarshallerPackage());

        writer.newLine();
        writer.write("public class AmqpMarshaller implements " + getPackagePrefix() + ".marshaller.AmqpMarshaller {");
        writer.newLine();

        writer.newLine();
        writer.write(tab(1) + "static final AmqpMarshaller SINGLETON = new AmqpMarshaller();");
        writer.newLine();

        writer.newLine();
        writer.write(tab(1) + "public static final AmqpVersion VERSION = new AmqpVersion((short)" + DEFINITIONS.get("MAJOR").getValue() + ", (short)" + DEFINITIONS.get("MINOR").getValue()
                + ", (short)" + DEFINITIONS.get("REVISION").getValue() + ");");
        writer.newLine();

        writer.newLine();
        writer.write(tab(1) + "public static final AmqpMarshaller getMarshaller() {");
        writer.newLine();
        writer.write(tab(2) + "return SINGLETON;");
        writer.newLine();
        writer.write(tab(1) + "}");
        writer.newLine();

        writer.newLine();
        writer.write(tab(1) + "private static final HashMap<Long, DescribedTypeMarshaller<?>> DESCRIBED_NUMERIC_TYPES = new HashMap<Long, DescribedTypeMarshaller<?>>();");
        writer.newLine();
        writer.write(tab(1) + "private static final HashMap<String, DescribedTypeMarshaller<?>> DESCRIBED_SYMBOLIC_TYPES = new HashMap<String, DescribedTypeMarshaller<?>>();");
        writer.newLine();
        writer.write(tab(1) + "static {");
        writer.newLine();

        for (AmqpClass amqpClass : TypeRegistry.getGeneratedTypes()) {
            if (amqpClass.isDescribed()) {
                writer.write(tab(2) + "DESCRIBED_NUMERIC_TYPES.put(" + amqpClass.getTypeMapping() + "Marshaller.NUMERIC_ID, " + amqpClass.getTypeMapping() + "Marshaller.SINGLETON);");
                writer.newLine();
                writer.write(tab(2) + "DESCRIBED_SYMBOLIC_TYPES.put(" + amqpClass.getTypeMapping() + "Marshaller.SYMBOLIC_ID, " + amqpClass.getTypeMapping() + "Marshaller.SINGLETON);");
                writer.newLine();
            }
        }
        writer.write(tab(1) + "}");
        writer.newLine();
        writer.newLine();
        writeJavaComment(writer, 1, "@return the protocol version of the marshaller");
        writer.write(tab(1) + "public final AmqpVersion getVersion() {");
        writer.newLine();
        writer.write(tab(2) + "return VERSION;");
        writer.newLine();
        writer.write(tab(1) + "}");
        writer.newLine();

        // Generate Handler methods:
        writer.newLine();
        writer.write(tab(1) + "public final " + TypeRegistry.any().typeMapping + " unmarshalType(DataInput in) throws IOException, AmqpEncodingError {");
        writer.newLine();
        writer.write(tab(2) + "return Encoder.unmarshalType(in);");
        writer.newLine();
        writer.write(tab(1) + "}");
        writer.newLine();

        writer.newLine();
        writer.write(tab(1) + "public final " + TypeRegistry.any().typeMapping + " decodeType(Buffer source) throws AmqpEncodingError {");
        writer.newLine();
        writer.write(tab(2) + "return Encoder.decode(source);");
        writer.newLine();
        writer.write(tab(1) + "}");
        writer.newLine();

        writer.newLine();
        writer.write(tab(1) + "final " + TypeRegistry.any().typeMapping + " decodeType(EncodedBuffer encoded) throws AmqpEncodingError {");
        writer.newLine();
        writer.write(tab(2) + "if(encoded.isDescribed()) {");
        writer.newLine();
        writer.write(tab(2) + "return decodeType(encoded.asDescribed());");
        writer.newLine();
        writer.write(tab(2) + "}");
        writer.newLine();

        writer.newLine();
        writer.write(tab(2) + "switch(encoded.getEncodingFormatCode()) {");
        writer.newLine();
        for (AmqpClass amqpClass : TypeRegistry.getGeneratedTypes()) {
            if (amqpClass.isPrimitive() && !amqpClass.getName().equals("*")) {

                writer.write(tab(2) + "//" + amqpClass.getTypeMapping() + " Encoded: ");
                writer.newLine();
                if (!amqpClass.hasMultipleEncodings() && !amqpClass.hasNonFixedEncoding()) {
                    writer.write(tab(2) + "case " + amqpClass.getTypeMapping() + "Marshaller.FORMAT_CODE: ");
                    writer.newLine();
                } else {
                    for (AmqpEncoding encoding : amqpClass.encodings) {

                        writer.write(tab(2) + "case (byte) " + encoding.getCode() + ":");
                        writer.newLine();
                    }
                }
                writer.write(tab(2) + "{");
                writer.newLine();
                writer.write(tab(3) + "return " + amqpClass.bufferMapping + ".create(" + amqpClass.getMarshaller() + ".createEncoded(encoded));");
                writer.newLine();
                writer.write(tab(2) + "}");
                writer.newLine();
            }
        }
        writer.write(tab(2) + "default: {");
        writer.newLine();
        writer.write(tab(3) + "//TODO: Create an unknown or any type");
        writer.newLine();
        writer.write(tab(3) + "throw new AmqpEncodingError(\"Unrecognized format code:\" + encoded.getEncodingFormatCode());");
        writer.newLine();
        writer.write(tab(2) + "}");
        writer.newLine();
        writer.write(tab(2) + "}");
        writer.newLine();
        writer.write(tab(1) + "}");
        writer.newLine();

        writer.newLine();
        writer.write(tab(1) + "final " + TypeRegistry.any().typeMapping + " decodeType(DescribedBuffer buffer) throws AmqpEncodingError {");
        writer.newLine();
        writer.write(tab(2) + TypeRegistry.any().typeMapping + " descriptor = decodeType(buffer.getDescriptorBuffer());");
        writer.newLine();
        writer.write(tab(2) + "//TODO might want to revisit whether or not the cast is needed here:");
        writer.newLine();
        writer.write(tab(2) + "DescribedTypeMarshaller<?> dtm = null;");
        writer.newLine();
        writer.write(tab(2) + "if(descriptor instanceof AmqpUlong) {");
        writer.newLine();
        writer.write(tab(3) + "dtm = DESCRIBED_NUMERIC_TYPES.get(((AmqpUlong)descriptor).getValue().longValue());");
        writer.newLine();
        writer.write(tab(2) + "}");
        writer.newLine();
        writer.write(tab(2) + "else if(descriptor instanceof AmqpSymbol) {");
        writer.newLine();
        writer.write(tab(3) + "dtm = DESCRIBED_SYMBOLIC_TYPES.get(((AmqpSymbol)descriptor).getValue());");
        writer.newLine();
        writer.write(tab(2) + "}");
        writer.newLine();
        writer.newLine();
        writer.write(tab(2) + "if(dtm != null) {");
        writer.newLine();
        writer.write(tab(3) + "return dtm.decodeDescribedType(descriptor, buffer);");
        writer.newLine();
        writer.write(tab(2) + "}");
        writer.newLine();
        writer.newLine();
        writer.write(tab(2) + "//TODO spec actuall indicates that we should be able to pass along unknown types. so we should just create");
        writer.newLine();
        writer.write(tab(2) + "//an placeholder type");
        writer.newLine();
        writer.write(tab(2) + "throw new AmqpEncodingError(\"Unrecognized described type:\" + descriptor);");
        writer.newLine();
        writer.write(tab(1) + "}");

        for (AmqpClass amqpClass : TypeRegistry.getGeneratedTypes()) {
            if (amqpClass.needsMarshaller()) {

                if (amqpClass.name.equals("*")) {
                    continue;
                }
                writer.newLine();
                writer.write(tab(1) + "public final Encoded<" + amqpClass.getValueMapping() + "> encode(" + amqpClass.getJavaType() + " data) throws AmqpEncodingError {");
                writer.newLine();
                writer.write(tab(2) + "return " + amqpClass.getJavaType() + "Marshaller.encode(data);");
                writer.newLine();
                writer.write(tab(1) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(1) + "public Encoded<" + amqpClass.getValueMapping() + "> decode" + amqpClass.getJavaType() + "(Buffer source, int offset) throws AmqpEncodingError {");
                writer.newLine();
                writer.write(tab(2) + "return " + amqpClass.getMarshaller() + ".createEncoded(source, offset);");
                writer.newLine();
                writer.write(tab(1) + "}");
                writer.newLine();

                writer.newLine();
                writer.write(tab(1) + "public Encoded<" + amqpClass.getValueMapping() + "> unmarshal" + amqpClass.getJavaType() + "(DataInput in) throws IOException, AmqpEncodingError {");
                writer.newLine();
                writer.write(tab(2) + "return " + amqpClass.getMarshaller() + ".createEncoded(in);");
                writer.newLine();
                writer.write(tab(1) + "}");
                writer.newLine();
            }
        }

        writer.write("}");
        writer.flush();
        writer.close();

    }

    private void generatePrimitiveEncoderInterface() throws IOException, UnknownTypeException {

        String outputPackage = getMarshallerPackage();
        File out = new File(outputDirectory + File.separator + outputPackage.replace(".", File.separator) + File.separator + "PrimitiveEncoder.java");

        BufferedWriter writer = new BufferedWriter(new FileWriter(out));

        writeJavaCopyWrite(writer);
        writer.write("package " + outputPackage + ";");
        writer.newLine();
        writer.newLine();

        TreeSet<String> imports = new TreeSet<String>();
        imports.add("java.io.DataOutput");
        writeMarshallerImports(writer, true, imports, getMarshallerPackage(), getPackagePrefix() + ".types");
        writer.newLine();

        writer.newLine();
        writer.write("public interface PrimitiveEncoder {");
        writer.newLine();

        HashSet<String> filters = new HashSet<String>();
        filters.add("*");
        filters.add("list");
        filters.add("map");

        // Write out encoding serializers:
        for (AmqpClass amqpClass : TypeRegistry.getGeneratedTypes()) {
            if (!amqpClass.isPrimitive() || filters.contains(amqpClass.getName())) {
                continue;
            }

            String javaType = amqpClass.getValueMapping().getJavaType();

            if (amqpClass.encodings != null) {

                if (amqpClass.hasNonZeroEncoding()) {
                    for (AmqpEncoding encoding : amqpClass.encodings) {
                        String encName = capFirst(toJavaName(amqpClass.name));
                        if (amqpClass.hasMultipleEncodings()) {
                            encName += capFirst(toJavaName(encoding.getName()));
                        }

                        writer.newLine();
                        writeJavaComment(writer, 1, "Writes a " + javaType + " encoded as " + encoding.getLabel());
                        writer.write(tab(1) + "public void write" + encName + "(" + javaType + " val, DataOutput buf) throws IOException, AmqpEncodingError;");
                        writer.newLine();

                        writeJavaComment(writer, 1, "Encodes a " + javaType + " as " + encoding.getLabel(), "", "The encoded data should be written into the supplied buffer at the given offset.");
                        writer.write(tab(1) + "public void encode" + encName + "(" + javaType + " val, Buffer buf, int offset) throws AmqpEncodingError;");
                        writer.newLine();

                        writer.newLine();
                        writeJavaComment(writer, 1, "Reads a " + javaType + " encoded as " + encoding.getLabel());
                        if (amqpClass.hasNonFixedEncoding()) {
                            writer.write(tab(1) + "public " + javaType + " read" + encName + "(int size, DataInput dis) throws IOException, AmqpEncodingError;");
                        } else {
                            writer.write(tab(1) + "public " + javaType + " read" + encName + "(DataInput dis) throws IOException, AmqpEncodingError;");
                        }
                        writer.newLine();

                        writer.newLine();
                        writeJavaComment(writer, 1, "Decodes a " + javaType + " encoded as " + encoding.getLabel());
                        if (amqpClass.hasNonFixedEncoding()) {
                            writer.write(tab(1) + "public " + javaType + " decode" + encName + "(Buffer encoded, int offset, int length) throws AmqpEncodingError;");
                        } else {
                            writer.write(tab(1) + "public " + javaType + " decode" + encName + "(Buffer encoded, int offset) throws AmqpEncodingError;");
                        }
                        writer.newLine();
                    }
                }
            }
        }

        writer.write("}");
        writer.newLine();
        writer.flush();
        writer.close();
    }

    private void generateTypeFactory() throws IOException, UnknownTypeException {

        String outputPackage = getPackagePrefix() + ".types";
        File out = new File(outputDirectory + File.separator + outputPackage.replace(".", File.separator) + File.separator + "TypeFactory.java");

        BufferedWriter writer = new BufferedWriter(new FileWriter(out));

        writeJavaCopyWrite(writer);
        writer.write("package " + outputPackage + ";");
        writer.newLine();
        writer.newLine();

        TreeSet<String> imports = new TreeSet<String>();
        writeTypeImports(writer, true, imports, getMarshallerPackage(), outputPackage);
        writer.newLine();

        writer.newLine();
        writer.write("public class TypeFactory {");
        writer.newLine();
        
        for (AmqpClass ac : TypeRegistry.getGeneratedTypes()) {
            if (ac.isAny() || ac.isEnumType()) {
                continue;
            }
            if (ac.isDescribed()) {
                writer.newLine();
                writeJavaComment(writer, 1, "Creates a " + ac.getTypeMapping());
                writer.write(tab(1) + "public static final " + ac.getTypeMapping() + " create" + ac.getTypeMapping() + "() {");
                writer.newLine();
                writer.write(tab(2) + "return new " + ac.getBeanMapping() + "();");
                writer.newLine();
                writer.write(tab(1) + "};");
                writer.newLine();
            } else {
                AmqpClass bt = ac.resolveBaseType();
                writer.newLine();
                writeJavaComment(writer, 1, "Creates a " + ac.getTypeMapping());
                writer.write(tab(1) + "public static final " + ac.getTypeMapping() + " create" + ac.getTypeMapping() + "(" + bt.getValueMapping() + " val) {");
                writer.newLine();
                writer.write(tab(2) + "return new " + ac.getBeanMapping() + "(val);");
                writer.newLine();
                writer.write(tab(1) + "}");
                writer.newLine();
                
                if(bt.getValueMapping().hasPrimitiveType())
                {
                    writeJavaComment(writer, 1, "Creates a " + ac.getTypeMapping());
                    writer.write(tab(1) + "public static final " + ac.getTypeMapping() + " create" + ac.getTypeMapping() + "(" + bt.getValueMapping().getPrimitiveType() + " val) {");
                    writer.newLine();
                    writer.write(tab(2) + "return new " + ac.getBeanMapping() + "(val);");
                    writer.newLine();
                    writer.write(tab(1) + "}");
                    writer.newLine();
                }
                
                if(bt.isMutable())
                {
                    writeJavaComment(writer, 1, "Creates an empty " + ac.getTypeMapping());
                    writer.write(tab(1) + "public static final " + ac.getTypeMapping() + " create" + ac.getTypeMapping() + "() {");
                    writer.newLine();
                    writer.write(tab(2) + "return new " + ac.getBeanMapping() + "();");
                    writer.newLine();
                    writer.write(tab(1) + "}");
                    writer.newLine();
                }
            }
        }
        writer.write("}");
        writer.newLine();
        writer.flush();
        writer.close();
    }

    private void writeMarshallerImports(BufferedWriter writer, boolean primitiveOnly, TreeSet<String> imports, String... packageFilters) throws IOException, UnknownTypeException {

        imports.add("java.io.DataInput");
        imports.add("java.io.IOException");
        imports.add(getPackagePrefix() + ".marshaller.AmqpEncodingError");

        writeTypeImports(writer, primitiveOnly, imports, packageFilters);
    }

    private void writeTypeImports(BufferedWriter writer, boolean primitiveOnly, TreeSet<String> imports, String... packageFilters) throws IOException, UnknownTypeException {
        HashSet<String> filters = new HashSet<String>();
        filters.add("java.lang");
        for (String filter : packageFilters) {
            filters.add(filter);
        }
        for (AmqpClass amqpClass : TypeRegistry.getGeneratedTypes()) {
            if (primitiveOnly && (!amqpClass.isPrimitive() || amqpClass.isList() || amqpClass.isMap())) {
                continue;
            }
            if (amqpClass.needsMarshaller()) {
                imports.add(amqpClass.getTypeMapping().getImport());
                JavaTypeMapping vMap = amqpClass.getValueMapping();
                if (vMap != null && vMap.getImport() != null) {
                    imports.add(vMap.getImport());
                }
            }
        }

        for (String i : imports) {
            if (!filters.contains(javaPackageOf(i))) {
                writer.write("import " + i + ";");
                writer.newLine();
            }

        }

    }

    private void generateClassFromType(Amqp source, Section section, Type type) throws Exception {
        AmqpClass amqpClass = new AmqpClass();
        amqpClass.parseFromType(this, source, section, type);
        TypeRegistry.addType(amqpClass);
    }

    public String getVersionPackageName() {
        return "v" + DEFINITIONS.get("MAJOR").getValue() + "_" + DEFINITIONS.get("MINOR").getValue() + "_" + DEFINITIONS.get("REVISION").getValue();
    }

    public String getMarshallerPackage() {
        return packagePrefix + ".marshaller." + getVersionPackageName();
    }

}
