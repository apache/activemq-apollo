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
package org.apache.activemq.apollo.mqtt;

import org.apache.activemq.apollo.broker.Broker$;
import org.apache.activemq.apollo.broker.Connector;
import org.apache.activemq.apollo.broker.protocol.ProtocolCodecFactory;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtdispatch.transport.ProtocolCodec;
import org.fusesource.mqtt.codec.MQTTProtocolCodec;

/**
 * Creates MqttCodec objects that encode/decode the
 * <a href="http://activemq.apache.org/mqtt/">Mqtt</a> protocol.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class MqttProtocolCodecFactory implements ProtocolCodecFactory.Provider {

    //
    // An MQTT CONNECT message has between 10-13 bytes:
    // Message Type     : 0x10 @ [0]
    // Remaining Length : Byte{1-4} @ [1]
    // Protocol Name    : 0x00 0x06 'M' 'Q' 'I' 's' 'd' 'p' @ [2|3|4|5]
    //
    static final String id = "mqtt";
    static final Buffer HEAD_MAGIC = new Buffer(new byte []{ 0x10 });
    static final Buffer MQTT31_TAIL_MAGIC = new Buffer(new byte []{ 0x00, 0x06, 'M', 'Q', 'I', 's', 'd', 'p'});
    static final Buffer MQTT311_TAIL_MAGIC = new Buffer(new byte []{ 0x00, 0x04, 'M', 'Q', 'T', 'T'});

    @Override
    public String id() {
        return id;
    }

    @Override
    public ProtocolCodec createProtocolCodec(Connector connector) {
        MQTTProtocolCodec rc = new MQTTProtocolCodec();
        rc.setBufferPools(Broker$.MODULE$.buffer_pools());
        return rc;
    }

    @Override
    public boolean isIdentifiable() {
        return true;
    }

    @Override
    public int maxIdentificaionLength() {
        return 13;
    }

    @Override
    public boolean matchesIdentification(Buffer header) {
        if (header.length < 10) {
          return false;
        } else {
          return header.startsWith(HEAD_MAGIC) && (
              header.indexOf(MQTT31_TAIL_MAGIC, 2) < 6 ||
              header.indexOf(MQTT311_TAIL_MAGIC, 2) < 6
          );
        }
    }

}
