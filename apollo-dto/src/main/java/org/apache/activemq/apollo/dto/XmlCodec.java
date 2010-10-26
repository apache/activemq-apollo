/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.apollo.dto;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.util.StreamReaderDelegate;
import java.io.*;
import java.net.URL;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class XmlCodec {

    /**
     * Changes ${property} with values from a properties object
     */
    static public class PropertiesFilter extends StreamReaderDelegate {

        static final Pattern pattern = Pattern.compile("\\$\\{([^\\}]+)\\}");
        private final Properties props;

        public PropertiesFilter(XMLStreamReader parent, Properties props) {
            super(parent);
            this.props = props;
        }

        public String getAttributeValue(int index) {
            return filter(super.getAttributeValue(index));
        }

        public String filter(String str) {
            int start = 0;
            while (true) {
                Matcher matcher = pattern.matcher(str);
                if (!matcher.find(start)) {
                    break;
                }
                String property = System.getProperty(matcher.group(1));
                if (property != null) {
                    str = matcher.replaceFirst(property);
                } else {
                    start = matcher.end();
                }
            }
            return str;
        }

    }

    private static final XMLInputFactory factory = XMLInputFactory.newInstance();
    private static final JAXBContext context;

    static {
        try {
            String path = "META-INF/services/org.apache.activemq.apollo/xml-packages.index";
            ClassLoader[] loaders = new ClassLoader[]{Thread.currentThread().getContextClassLoader()};

            HashSet<String> names = new HashSet<String>();
            for (ClassLoader loader : loaders) {
                try {
                    Enumeration<URL> resources = loader.getResources(path);

                    while (resources.hasMoreElements()) {
                        URL url = resources.nextElement();
                        Properties p = loadProperties(url.openStream());
                        Enumeration<Object> keys = p.keys();
                        while (keys.hasMoreElements()) {
                            names.add((String) keys.nextElement());
                        }
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            String packages = "";
            for ( String p : names) {
                if( p.length() !=0 ) {
                    p += ":";
                }
                packages += p;
            }
            context = JAXBContext.newInstance(packages);

        } catch (JAXBException e) {
            throw new RuntimeException(e);
        }
    }

    static private Properties loadProperties(InputStream is) {
        if (is == null) {
            return null;
        }
        try {
            Properties p = new Properties();
            p.load(is);
            return p;
        } catch (Exception e) {
            return null;
        } finally {
            try {
                is.close();
            } catch (Throwable e) {
            }
        }
    }


    static public BrokerDTO unmarshalBrokerDTO(URL url) throws IOException, XMLStreamException, JAXBException {
        return unmarshalBrokerDTO(url, null);
    }

    static public BrokerDTO unmarshalBrokerDTO(URL url, Properties props) throws IOException, XMLStreamException, JAXBException {
        return unmarshalBrokerDTO(url.openStream(), props);
    }

    static public BrokerDTO unmarshalBrokerDTO(InputStream is) throws IOException, XMLStreamException, JAXBException {
        return unmarshalBrokerDTO(is, null);
    }

    static public BrokerDTO unmarshalBrokerDTO(InputStream is, Properties props) throws IOException, XMLStreamException, JAXBException {
        if (is == null) {
            throw new IllegalArgumentException("input stream was null");
        }
        try {
            XMLStreamReader reader = factory.createXMLStreamReader(is);
            if (props != null) {
                reader = new PropertiesFilter(reader, props);
            }
            Unmarshaller unmarshaller = context.createUnmarshaller();
            return (BrokerDTO) unmarshaller.unmarshal(reader);
        } finally {
            is.close();
        }
    }

    static public void marshalBrokerDTO(BrokerDTO in, OutputStream os, boolean format) throws JAXBException {
        Marshaller marshaller = context.createMarshaller();
        if( format ) {
            marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, java.lang.Boolean.TRUE);
        }
        marshaller.marshal(in, new OutputStreamWriter(os));
    }


}
