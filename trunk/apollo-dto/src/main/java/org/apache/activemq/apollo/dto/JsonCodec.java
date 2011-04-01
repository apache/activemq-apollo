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

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.ObjectMapper;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.ByteArrayOutputStream;

import javax.xml.bind.JAXBException;
import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

/**
 *
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class JsonCodec {
    private static ObjectMapper mapper = new ObjectMapper();

    static public <T> T decode(Buffer buffer, Class<T> type) throws IOException {
        return mapper.readValue(buffer.in(), type);
    }

    static public Buffer encode(Object value) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        mapper.writeValue(baos, value);
        return baos.toBuffer();
    }

}
