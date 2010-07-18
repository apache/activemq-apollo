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
package org.apache.activemq.apollo.transport;

import org.apache.activemq.apollo.util.JavaClassFinder;
import org.fusesource.hawtbuf.Buffer;

import java.util.HashMap;
import java.util.List;

public class ProtocolCodecFactory {

    public static interface Provider {

        String protocol();

        /**
         * @return an instance of the wire format.
         *
         */
        ProtocolCodec createProtocolCodec();

        /**
         * @return true if this wire format factory is identifiable. An identifiable
         * protocol will first write a easy to identify header to the stream
         */
        boolean isIdentifiable();

        /**
         * @return Returns the maximum length of the header used to discriminate the wire format if it
         * {@link #isIdentifiable()}
         * @throws UnsupportedOperationException If {@link #isIdentifiable()} is false
         */
        int maxIdentificaionLength();

        /**
         * Called to test if this protocol matches the identification header.
         *
         * @param buffer The byte buffer representing the header data read so far.
         * @return true if the Buffer matches the protocol format header.
         */
        boolean matchesIdentification(Buffer buffer);

    }

    static public HashMap<String, Provider> providers = new HashMap<String, Provider>();

    static {
        JavaClassFinder<Provider> finder = new JavaClassFinder<Provider>("META-INF/services/org.apache.activemq.apollo/protocol-codec-factory.index");
        for( Provider provider: finder.new_instances() ) {
            providers.put(provider.protocol(), provider);
        }
    }

    /**
     * Gets the provider.
     */
    public static ProtocolCodecFactory.Provider get(String name) {
        return providers.get(name);
    }


}


