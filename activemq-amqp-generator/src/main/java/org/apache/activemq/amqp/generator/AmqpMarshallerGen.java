package org.apache.activemq.amqp.generator;

import static org.apache.activemq.amqp.generator.Utils.capFirst;
import static org.apache.activemq.amqp.generator.Utils.tab;
import static org.apache.activemq.amqp.generator.Utils.toJavaName;
import static org.apache.activemq.amqp.generator.Utils.writeJavaComment;
import static org.apache.activemq.amqp.generator.Utils.writeJavaCopyWrite;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;

/**
 * Generates the AMQPMarshaller template:
 * 
 * @author cmacnaug
 */
public class AmqpMarshallerGen {

    public static final String MARSHALLER_CLASS_NAME = "AbstractAmqpMarshaller";

    public static void genererate(Generator generator) throws IOException, UnknownTypeException {
        File file = new File(generator.getOutputDirectory() + File.separator + new String(generator.getPackagePrefix() + "." + MARSHALLER_CLASS_NAME).replace(".", File.separator) + ".java");
        file.getParentFile().mkdirs();
        if (file.exists()) {
            file.delete();
        }
        BufferedWriter writer = new BufferedWriter(new FileWriter(file));

        writeJavaCopyWrite(writer);

        writer.write("package " + generator.getPackagePrefix() + ";\n");
        writer.newLine();

        writeImports(writer, generator);

        writer.newLine();
        writer.write("public class " + MARSHALLER_CLASS_NAME + " {");
        writer.newLine();

        // Write out encoding enums:
        writer.newLine();
        writer.write(tab(1) + "///////////////////////////////////////////////////////////////");
        writer.write(tab(1) + "//Encodings:                                                 //");
        writer.write(tab(1) + "///////////////////////////////////////////////////////////////");
        for (AmqpClass c : TypeRegistry.getGeneratedTypes()) {
            if (!c.isPrimitive()) {
                continue;
            }
        }

        writer.newLine();
        writer.write(tab(1) + "///////////////////////////////////////////////////////////////");
        writer.write(tab(1) + "//Marshallers:                                               //");
        writer.write(tab(1) + "///////////////////////////////////////////////////////////////");

        // Write out encoding serializers:
        for (AmqpClass c : TypeRegistry.getGeneratedTypes()) {
            if (!c.isPrimitive()) {
                continue;
            }
            if (c.encodings != null) {
                writeEncodingSerializers(c, writer);
            }
        }

        writer.newLine();
        writer.write("}");
        writer.flush();
        writer.close();
    }

    private static boolean writeImports(BufferedWriter writer, Generator generator) throws IOException, UnknownTypeException {

        HashSet<String> imports = new HashSet<String>();

        for (AmqpClass c : TypeRegistry.getGeneratedTypes()) {
            if (!c.isPrimitive()) {
                continue;
            }
            
            for (AmqpField field : c.fields.values()) {
                
                AmqpClass fieldType = field.resolveAmqpFieldType();
                imports.add(fieldType.resolveValueMapping().getPackageName());
            }
            imports.add(c.getTypeMapping().getPackageName());
        }
        imports.add("java.io");
        imports.add(generator.getPackagePrefix() + ".types");
        imports.remove("java.lang");

        boolean ret = false;

        for (String toImport : imports) {
            ret = true;
            writer.write("import " + toImport + ".*;");
            writer.newLine();
        }
        return ret;
    }

    private static void writeEncodingSerializers(AmqpClass amqpClass, BufferedWriter writer) throws IOException, UnknownTypeException {

        String javaType = TypeRegistry.getJavaType(amqpClass.name);

        if (amqpClass.encodings.size() > 1) {
            writer.newLine();
            writeJavaComment(writer, 1, "Chooses a " + amqpClass.getEncodingName(true) + " for the given " + javaType);
            writer.write(tab(1) + "public static final " + amqpClass.getEncodingName(true) + " choose" + capFirst(amqpClass.name) + "Encoding(" + javaType + " val) throws IOException {");
            writeUnimplementedMethodBody(writer, 1);

            writer.newLine();
            writeJavaComment(writer, 1, "Gets the encoded size of " + javaType + " with the given encoding");
            writer.write(tab(1) + "public static final int getEncodedSizeOf" + capFirst(toJavaName(amqpClass.name)) + "(" + javaType + " value, " + amqpClass.getEncodingName(true)
                    + " encoding) throws IOException {");
            writeUnimplementedMethodBody(writer, 1);
            writer.newLine();

            if (amqpClass.hasNonZeroEncoding()) {
                writer.newLine();
                writeJavaComment(writer, 1, "Writes a " + javaType + " with the given encoding");
                writer.write(tab(1) + "public static final void write" + capFirst(toJavaName(amqpClass.name)) + "(" + javaType + " val, " + amqpClass.getEncodingName(true)
                        + " encoding, DataOutputStream dos) throws IOException {");
                writeUnimplementedMethodBody(writer, 1);

                writer.newLine();
                writeJavaComment(writer, 1, "Reads a " + javaType + " with the given encoding");
                if(amqpClass.hasCompoundEncoding() || amqpClass.hasArrayEncoding() ||  amqpClass.hasVariableEncoding())
                {
                    writer.write(tab(1) + "public static final " + javaType + " read" + capFirst(toJavaName(amqpClass.name)) + "(" + amqpClass.getEncodingName(true)
                            + " encoding, int size, int count, DataInputStream dis) throws IOException {");
                }
                else {
                    writer.write(tab(1) + "public static final " + javaType + " read" + capFirst(toJavaName(amqpClass.name)) + "(" + amqpClass.getEncodingName(true)
                            + " encoding, DataInputStream dis) throws IOException {");
                }
                writeUnimplementedMethodBody(writer, 1);
            }
        } else {
            AmqpEncoding encoding = amqpClass.encodings.getFirst();
            // Don't need a serializer if the width is 0:
            if (amqpClass.hasNonZeroEncoding()) {

                writer.newLine();
                writeJavaComment(writer, 1, "Writes a " + javaType + " encoded as " + encoding.getLabel());
                writer.write(tab(1) + "public static final void write" + capFirst(toJavaName(amqpClass.name)) + "(" + javaType + " val, DataOutputStream dos) throws IOException {");
                writeUnimplementedMethodBody(writer, 1);

                writer.newLine();
                writeJavaComment(writer, 1, "Reads a " + javaType + " encoded as " + encoding.getLabel());
                if (amqpClass.hasNonFixedEncoding()) {
                    writer.write(tab(1) + "public static final " + javaType + " read" + capFirst(toJavaName(amqpClass.name)) + "(int size, int count, DataInputStream dis) throws IOException {");
                } else {
                    writer.write(tab(1) + "public static final " + javaType + " read" + capFirst(toJavaName(amqpClass.name)) + "(DataInputStream dis) throws IOException {");
                }
                writeUnimplementedMethodBody(writer, 1);
                
            }
        }
    }
    
    private static void writeUnimplementedMethodBody(BufferedWriter writer, int indent) throws IOException
    {
        writer.newLine();
        writer.write(tab(indent + 1) + "//TODO: Implement");
        writer.newLine();
        writer.write(tab(indent + 1) + "throw new UnsupportedOperationException(\"not implemented\");");
        writer.newLine();
        writer.write(tab(indent) + "}");
    }
    
}
