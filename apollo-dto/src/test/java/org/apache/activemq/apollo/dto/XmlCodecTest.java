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

import org.junit.Test;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import java.io.InputStream;
import java.io.StringReader;

import static junit.framework.Assert.*;



/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */

public class XmlCodecTest {

    private InputStream resource(String path) {
        return getClass().getResourceAsStream(path);
    }

    @Test
    public void validateXSD() throws SAXException {
        SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        Schema schema = factory.newSchema(XmlCodec.class.getResource("apollo.xsd"));
        assertNotNull(schema);
    }

    @Test
    public void unmarshalling() throws Exception {
        BrokerDTO dto = XmlCodec.decode(BrokerDTO.class, resource("XmlCodecTest.xml"));
        assertNotNull(dto);

        assertEquals(1, dto.other.size());

        VirtualHostDTO host = dto.virtual_hosts.get(0);
        assertEquals("vh-local", host.id);
        assertEquals("localhost", host.host_names.get(0));
        assertEquals("example.com", host.host_names.get(1));

        assertEquals("queue1", host.queues.get(0).id);
        assertEquals("topic1", host.topics.get(0).id);
        assertEquals("durable_subscription1", host.dsubs.get(0).id);

        AcceptingConnectorDTO connector = (AcceptingConnectorDTO)dto.connectors.get(0);
        assertNotNull(connector);

    }


    @Test
    public void marshalling() throws Exception {
        BrokerDTO broker = new BrokerDTO();

        VirtualHostDTO host = new VirtualHostDTO();
        host.id = "vh-local";
        host.host_names.add("localhost");
        host.host_names.add("example.com");
        broker.virtual_hosts.add(host);

        AcceptingConnectorDTO connector = new AcceptingConnectorDTO();
        connector.id = "port-61616";
        connector.bind = "tcp://0.0.0.0:61616";
        broker.connectors.add(connector);

        XmlCodec.encode(broker, System.out, true);

    }

}
