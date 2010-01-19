package org.apache.activemq.amqp.generator;

import static org.apache.activemq.amqp.generator.Utils.*;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.StringTokenizer;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.Source;
import javax.xml.transform.sax.SAXSource;

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
    private String packagePrefix;

    public static final HashSet<String> CONTROLS = new HashSet<String>();
    public static final HashSet<String> COMMANDS = new HashSet<String>();
    public static final LinkedList<AmqpDefinition> DEFINITIONS = new LinkedList<AmqpDefinition>();

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

                    System.out.println("Scanning: " + section.getName());
                    for (Object docOrDefinitionOrType : section.getDocOrDefinitionOrType()) {
                        if (docOrDefinitionOrType instanceof Type) {
                            generateClassFromType(amqp, section, (Type) docOrDefinitionOrType);
                        } else if (docOrDefinitionOrType instanceof Definition) {
                            DEFINITIONS.add(new AmqpDefinition((Definition) docOrDefinitionOrType));
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

        generateCommandHandler();
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
        for (AmqpDefinition def : DEFINITIONS) {
            writer.newLine();
            def.writeJavaDoc(writer, 1);
            writer.write(tab(1) + "public static final String " + capFirst(toJavaConstant(def.getName())) + " = \"" + def.getValue() + "\";");
            writer.newLine();
        }

        writer.write("}");
        writer.flush();
        writer.close();
    }

    private void generateClassFromType(Amqp source, Section section, Type type) throws Exception {
        AmqpClass amqpClass = new AmqpClass();
        amqpClass.parseFromType(this, source, section, type);
        TypeRegistry.addType(amqpClass);
        System.out.println("Found: " + amqpClass);
    }

}
