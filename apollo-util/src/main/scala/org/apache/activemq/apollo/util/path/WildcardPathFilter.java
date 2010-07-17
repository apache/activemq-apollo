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

package org.apache.activemq.apollo.util.path;

import java.util.ArrayList;

import org.fusesource.hawtbuf.AsciiBuffer;


/**
 * Matches messages which contain wildcards like "A.B.*.*"
 *
 * @version $Revision: 1.2 $
 */
public class WildcardPathFilter extends PathFilter {

    private AsciiBuffer[] prefixes;

    /**
     * An array of paths containing * characters
     *
     * @param paths
     */
    public WildcardPathFilter(ArrayList<AsciiBuffer> paths) {
        this.prefixes = new AsciiBuffer[paths.size()];
        for (int i = 0; i < paths.size(); i++) {
        	AsciiBuffer prefix = paths.get(i);
            if (!prefix.equals(PathFilter.ANY_CHILD)) {
                this.prefixes[i] = prefix;
            }
        }
    }

    public boolean matches(AsciiBuffer path) {
        ArrayList<AsciiBuffer> parts = PathSupport.parse(path);
        int length = prefixes.length;
        if (parts.size() == length) {
            for (int i = 0; i < length; i++) {
            	AsciiBuffer prefix = prefixes[i];
                if (prefix != null && !prefix.equals(parts.get(i))) {
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
        return PathSupport.toString(t);
    }

    public String toString() {
        return super.toString() + "[path: " + getText() + "]";
    }

    public boolean isWildcard() {
        return true;
    }
}
