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
 * Helper class for decomposing a AsciiBuffer based path into it path component parts.
 * 
 * @version $Revision: 1.3 $
 */
public final class PathSupport {
    protected static final byte SEPARATOR = '.';

    private PathSupport() {    
    }
    
    public static ArrayList<AsciiBuffer> parse(AsciiBuffer subject) {
    	ArrayList<AsciiBuffer> list = new ArrayList<AsciiBuffer>(10);
        int previous = 0;
        int lastIndex = subject.getLength() - 1;
        while (true) {
            int idx = subject.indexOf(SEPARATOR, previous);
            if (idx < 0) {
            	AsciiBuffer buffer = subject.slice(previous, lastIndex + 1).ascii();
                list.add(buffer);
                break;
            }
        	AsciiBuffer buffer = subject.slice(previous, idx).ascii();
            list.add(buffer);
            previous = idx + 1;
        }
        return list;
    }

    /**
     * Converts the paths to a single String seperated by dots.
     * 
     * @param paths
     * @return
     */
    public static String toString(ArrayList<AsciiBuffer> paths) {
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < paths.size(); i++) {
            if (i > 0) {
                buffer.append(SEPARATOR);
            }
            AsciiBuffer path = paths.get(i);
            if (path == null) {
                buffer.append("*");
            } else {
                buffer.append(path);
            }
        }
        return buffer.toString();
    }
}
