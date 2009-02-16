package org.apache.activemq.wireformat;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.activemq.wireformat.WireFormat;

public interface StatefulWireFormat extends WireFormat{

    /**
     * Writes a command to the target buffer, returning false if
     * the command couldn't entirely fit into the target. 
     * @param command
     * @param target
     * @return
     */
    public boolean marshal(Object command, ByteBuffer target) throws IOException;
    
    /**
     * Unmarshals an object. When the object is read it is returned.
     * @param source
     * @return The object when unmarshalled, null otherwise
     */
    public Object unMarshal(ByteBuffer source) throws IOException;
    
    
}
