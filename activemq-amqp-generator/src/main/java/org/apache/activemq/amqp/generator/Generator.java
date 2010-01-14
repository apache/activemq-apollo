package org.apache.activemq.amqp.generator;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.Source;
import javax.xml.transform.sax.SAXSource;

import org.apache.activemq.amqp.generator.jaxb.schema.Amqp;
import org.apache.activemq.amqp.generator.jaxb.schema.Section;
import org.apache.activemq.amqp.generator.jaxb.schema.Type;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;

public class Generator {

    private Log LOG = LogFactory.getLog(Generator.class);

    private String[] inputFiles;
    private String outputDirectory;
    private String packagePrefix;

    private TypeRegistry typeRegistry = new TypeRegistry();

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
            // JAXB has some namespace handling problems:
            Unmarshaller unmarshaller = jc.createUnmarshaller();
            SAXParserFactory parserFactory;
            parserFactory = SAXParserFactory.newInstance();
            parserFactory.setNamespaceAware(false);
            XMLReader reader = parserFactory.newSAXParser().getXMLReader();
            Source er = new SAXSource(reader, new InputSource(inputFile));
            // Amqp amqp = (Amqp) unmarshaller.unmarshal(new StreamSource(new
            // File(inputFile)), Amqp.class).getValue();
            Amqp amqp = (Amqp) unmarshaller.unmarshal(er);
            List<Object> list = amqp.getDocOrSection();

            // Scan document:
            for (Object docOrSection : amqp.getDocOrSection()) {
                if (docOrSection instanceof Section) {
                    Section section = (Section) docOrSection;
                    System.out.println("Scanning: " + section.getName());
                    for (Object docOrDefinitionOrType : section.getDocOrDefinitionOrType()) {
                        if (docOrDefinitionOrType instanceof Type) {
                            generateClassFromType(amqp.getName(), (Type) docOrDefinitionOrType);
                        }
                    }
                }
            }
        }

        // Copy handcoded:
        String[] handcoded = new String[] { "AmqpMarshaller", "AmqpType"};
        for (String javaFile : handcoded) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(this.getClass().getClassLoader().getResourceAsStream("org/apache/activemq/amqp/generator/handcoded/" + javaFile + ".jtemp")));
            String line = reader.readLine();
            line = line.replace("org.apache.activemq.amqp.generator.handcoded", packagePrefix);

            File out = new File(this.outputDirectory + File.separator + packagePrefix.replace(".", File.separator) + File.separator + javaFile + ".java");
            out.getParentFile().mkdirs();
            BufferedWriter writer = new BufferedWriter(new FileWriter(out));

            while (line != null) {
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

    }

    private void generateClassFromType(String source, Type type) throws Exception {
        AmqpClass amqpClass = new AmqpClass();
        amqpClass.parseFromType(this, source, type);
        TypeRegistry.addType(amqpClass);
        System.out.println("Found: " + amqpClass);
    }
}
