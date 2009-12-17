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
package org.apache.activemq.queue.actor.transport;

import org.apache.activemq.dispatch.Dispatcher;
import org.apache.activemq.queue.actor.transport.pipe.PipeTransportFactory;



/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class TransportFactorySystem {

    public static Transport connect(Dispatcher dispatcher, String connectUri) {
        return new PipeTransportFactory().connect(dispatcher, connectUri);
    }

    public static TransportServer bind(Dispatcher dispatcher, String bindUri) {
        return new PipeTransportFactory().bind(dispatcher, bindUri);
    }

}
