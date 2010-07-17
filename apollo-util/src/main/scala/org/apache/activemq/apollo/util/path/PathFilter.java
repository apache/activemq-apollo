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
 * Represents a filter which only operates on a path
 * 
 * @version $Revision: 1.3 $
 */
public abstract class PathFilter {

    public static final AsciiBuffer ANY_DESCENDENT = new AsciiBuffer(">");
    public static final AsciiBuffer ANY_CHILD = new AsciiBuffer("*");
    
    public abstract boolean matches(AsciiBuffer path);

    public static PathFilter parseFilter(AsciiBuffer path) {
    	if( containsWildCards(path) ) { 
	        ArrayList<AsciiBuffer> paths = PathSupport.parse(path);
	        int idx = paths.size() - 1;
	        if (idx >= 0) {
	        	AsciiBuffer lastPath = paths.get(idx);
	            if (lastPath.equals(ANY_DESCENDENT)) {
	                return new PrefixPathFilter(paths);
	            } else {
	                while (idx >= 0) {
	                    lastPath = paths.get(idx--);
	                    if (lastPath.equals(ANY_CHILD)) {
	                        return new WildcardPathFilter(paths);
	                    }
	                }
	            }
	        }
    	}

        // if none of the paths contain a wildcard then use equality
        return new SimplePathFilter(path);
    }
    
    public static boolean containsWildCards(AsciiBuffer path) {
    	byte b1 = ANY_DESCENDENT.getData()[0];
    	byte b2 = ANY_CHILD.getData()[0];
    	
    	byte[] data = path.getData();
    	int length = path.getOffset()+path.getLength();
		for (int i = path.getOffset(); i < length; i++) {
			if( data[i] == b1 || data[i]==b2 ) {
				return true;
			}
		}
		return false;
    }
    
}
