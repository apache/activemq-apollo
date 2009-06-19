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

package org.apache.activemq.apollo.broker.wildcard;

import java.util.ArrayList;

import org.apache.activemq.apollo.broker.Destination;
import org.apache.activemq.protobuf.AsciiBuffer;



/**
 * Matches messages which contain wildcards like "A.B.*.*"
 *
 * @version $Revision: 1.2 $
 */
public class WildcardDestinationFilter extends DestinationFilter {

    private AsciiBuffer[] prefixes;

    /**
     * An array of paths containing * characters
     *
     * @param paths
     */
    public WildcardDestinationFilter(ArrayList<AsciiBuffer> paths) {
        this.prefixes = new AsciiBuffer[paths.size()];
        for (int i = 0; i < paths.size(); i++) {
        	AsciiBuffer prefix = paths.get(i);
            if (!prefix.equals(DestinationFilter.ANY_CHILD)) {
                this.prefixes[i] = prefix;
            }
        }
    }

    public boolean matches(Destination destination) {
        ArrayList<AsciiBuffer> path = DestinationPath.parse(destination);
        int length = prefixes.length;
        if (path.size() == length) {
            for (int i = 0; i < length; i++) {
            	AsciiBuffer prefix = prefixes[i];
                if (prefix != null && !prefix.equals(path.get(i))) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }


    public String getText() {
    	ArrayList<AsciiBuffer> t = new ArrayList<AsciiBuffer>(prefixes.length);
    	t.toArray(prefixes);
        return DestinationPath.toString(t);
    }

    public String toString() {
        return super.toString() + "[destination: " + getText() + "]";
    }

    public boolean isWildcard() {
        return true;
    }
}
