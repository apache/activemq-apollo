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
package example;

import org.apache.activemq.apollo.broker.Broker;
import org.apache.activemq.apollo.broker.store.leveldb.dto.*;
import org.apache.activemq.apollo.dto.*;

import java.io.File;

/**
 * Example of how to setup an embedded broker.
 */
public class EmbeddedBroker {

    public static void main(String[] args) throws Exception {

        //
        // Creating and initially configuring the broker.
        Broker broker = new Broker();
        broker.setTmp(new File("./tmp"));
        broker.setConfig(createConfig());

        //
        // The broker starts asynchronously. The runnable is invoked once
        // the broker if fully started.
        System.out.println("Starting the broker.");
        broker.start(new Runnable(){
            public void run() {
                System.out.println("The broker has now started.");
                System.out.println("Press enter to change the broker port...");
            }
        });

        System.in.read();
        System.out.println("Updating the broker configuration.");
        
        //
        // The configuration update also occurs asnyc.
        broker.update(createUpdate(), new Runnable() {
            public void run() {
                System.out.println("The configuration has been applied.");
                System.out.println("Press enter to stop the broker...");
            }
        });

        System.in.read();
        System.out.println("Stopping the broker.");
        
        //
        // The broker stops asynchronously. The runnable is invoked once
        // the broker if fully stopped.
        broker.stop(new Runnable(){
            public void run() {
                System.out.println("The broker has now stopped.");
            }
        });
        
    }

    /**
     * Builds a simple configuration model with just plain Java.  Corresponds 1 to 1 with
     * the XML configuration model.  See the Apollo user guide for more details.
     * @return
     */
    private static BrokerDTO createConfig() {
        BrokerDTO broker = new BrokerDTO();

        // Brokers support multiple virtual hosts.
        VirtualHostDTO host = new VirtualHostDTO();
        host.id = "localhost";
        host.host_names.add("localhost");
        host.host_names.add("127.0.0.1");

        // The message store is configured on the virtual host.
        LevelDBStoreDTO store = new LevelDBStoreDTO();
        store.directory = new File("./data");
        host.store = store;

        broker.virtual_hosts.add(host);

        //
        // Control which ports and protocols the broker binds and accepts
        AcceptingConnectorDTO connector = new AcceptingConnectorDTO();
        connector.id = "tcp";
        connector.bind = "tcp://0.0.0.0:61613";
        broker.connectors.add(connector);

        //
        // Fires up the web admin console on HTTP.
        WebAdminDTO webadmin = new WebAdminDTO();
        webadmin.bind = "http://0.0.0.0:8080";
        broker.web_admins.add(webadmin);

        return broker;
    }

    private static BrokerDTO createUpdate() {
        BrokerDTO broker = createConfig();

        // Lets change the port.
        AcceptingConnectorDTO connector = (AcceptingConnectorDTO) broker.connectors.get(0);
        connector.bind = "tcp://0.0.0.0:61614";

        return broker;
    }

}
