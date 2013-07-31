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

import org.apache.activemq.apollo.broker.Connector;
import org.apache.activemq.apollo.broker.DestinationParser;
import org.apache.activemq.apollo.broker.protocol.Protocol;
import org.apache.activemq.apollo.broker.protocol.ProtocolHandler;
import org.fusesource.hawtbuf.AsciiBuffer;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class MqttProtocol extends MqttProtocolCodecFactory implements Protocol {

    static final DestinationParser destination_parser = new DestinationParser();
    static final AsciiBuffer PROTOCOL_ID = new AsciiBuffer(id);
    static {
        destination_parser.queue_prefix_$eq(null);
        destination_parser.topic_prefix_$eq(null);
        destination_parser.path_separator_$eq("/");
        destination_parser.any_child_wildcard_$eq("+");
        destination_parser.any_descendant_wildcard_$eq("#");
        destination_parser.dsub_prefix_$eq(null);
        destination_parser.temp_queue_prefix_$eq(null);
        destination_parser.temp_topic_prefix_$eq(null);
        destination_parser.destination_separator_$eq(null);
        destination_parser.regex_wildcard_end_$eq(null);
        destination_parser.regex_wildcard_end_$eq(null);
        destination_parser.part_pattern_$eq(null);
    }


    @Override
    public ProtocolHandler createProtocolHandler(Connector connector) {
        return new MqttProtocolHandler();
    }
}
