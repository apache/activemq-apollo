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

import java.io.InputStream;
import static junit.framework.Assert.*;



/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */

public class XmlCodecTest {

    private InputStream resource(String path) {
        return getClass().getResourceAsStream(path);
    }

    @Test
    public void unmarshalling() throws Exception {
        BrokerDTO dto = XmlCodec.unmarshalBrokerDTO(resource("XmlCodecTest.xml"));
        assertNotNull(dto);
        assertEquals(1, dto.connectors.size());
        ConnectorDTO connector = dto.connectors.get(0);
        assertEquals(1, connector.protocols.size());
        ProtocolDTO stomp = connector.protocols.get(0);
        assertTrue(stomp instanceof StompDTO);
        assertEquals("JMSXUserID", ((StompDTO) stomp).add_user_header);


        VirtualHostDTO host = dto.virtual_hosts.get(0);
        assertNotNull(host.acl);
        assertEquals("vh-local", host.id);
        assertEquals("localhost", host.host_names.get(0));
        assertEquals("example.com", host.host_names.get(1));

        assertNotNull(dto.acl);
        assertTrue(dto.acl.admins.contains(new PrincipalDTO("hiram")));
        assertTrue(dto.acl.admins.contains(new PrincipalDTO("james")));
        assertTrue(dto.acl.admins.contains(new PrincipalDTO("admins", "org.apache.activemq.jaas.GroupPrincipal")));
    }


    @Test
    public void marshalling() throws Exception {
        BrokerDTO broker = new BrokerDTO();

        VirtualHostDTO host = new VirtualHostDTO();
        host.id = "vh-local";
        host.host_names.add("localhost");
        host.host_names.add("example.com");
        broker.virtual_hosts.add(host);

        ConnectorDTO connector = new ConnectorDTO();
        connector.id = "port-61616";
        connector.bind = "tcp://0.0.0.0:61616";
        broker.connectors.add(connector);

        XmlCodec.marshalBrokerDTO(broker, System.out, true);

    }

}
