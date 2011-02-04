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

import org.apache.activemq.apollo.util.ClassFinder;
import org.apache.activemq.apollo.util.Module;
import org.apache.activemq.apollo.util.ModuleRegistry;

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
                String group = matcher.group(1);
                String property = props.getProperty(group);
                if (property != null) {
                    str = matcher.replaceFirst(Matcher.quoteReplacement(property));
                } else {
                    start = matcher.end();
                }
            }
            return str;
        }

    }

    private static final XMLInputFactory factory = XMLInputFactory.newInstance();
    volatile public static JAXBContext _context;

    private static JAXBContext context() throws JAXBException {
        JAXBContext rc = _context;
        if( rc==null ) {
            rc = _context = createContext();
        }
        return rc;
    }

    private static JAXBContext createContext() throws JAXBException {
        HashSet<String> names = new HashSet<String>();
        for( Module m: ModuleRegistry.jsingletons()) {
            for( String p:m.xml_packages() ) {
                names.add(p);
            }
        }

        String packages = "";
        for ( String p : names) {
            if( packages.length() !=0 ) {
                packages += ":";
            }
            packages += p;
        }
        return JAXBContext.newInstance(packages);
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
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(ClassFinder.class_loader());
            if (is == null) {
                throw new IllegalArgumentException("input stream was null");
            }
            try {
                XMLStreamReader reader = factory.createXMLStreamReader(is);
                if (props != null) {
                    reader = new PropertiesFilter(reader, props);
                }
                Unmarshaller unmarshaller = context().createUnmarshaller();
                return (BrokerDTO) unmarshaller.unmarshal(reader);
            } finally {
                is.close();
            }

        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    static public void marshalBrokerDTO(BrokerDTO in, OutputStream os, boolean format) throws JAXBException {
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(ClassFinder.class_loader());
            Marshaller marshaller = context().createMarshaller();
            if( format ) {
                marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, java.lang.Boolean.TRUE);
            }
            marshaller.marshal(in, new OutputStreamWriter(os));
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }


}
