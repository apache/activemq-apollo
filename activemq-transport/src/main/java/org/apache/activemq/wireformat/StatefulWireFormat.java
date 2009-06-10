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
package org.apache.activemq.wireformat;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.activemq.wireformat.WireFormat;

public interface StatefulWireFormat extends WireFormat{

    
    /**
     * Writes a command to the sub buffer, returning false if
     * the command couldn't entirely fit into the target buffer. In this case
     * the caller should supply an additional buffer or drain the current
     * until the command is marshalled. 
     *  
     * @param command The command to marshal.
     * @param target The target buffer.
     * @return true if the command was fully marshalled. 
     * 
     * @throws IOException if there is an error writing the buffer. 
     */
    public boolean marshal(Object command, ByteBuffer target) throws IOException;
    
    /**
     * Unmarshals an object. When the object is read it is returned.
     * @param source
     * @return The object when unmarshalled, null otherwise
     */
    public Object unMarshal(ByteBuffer source) throws IOException;
    
    
}
