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

import java.util.Collection;

import org.apache.activemq.apollo.broker.Destination;
import org.apache.activemq.filter.BooleanExpression;
import org.apache.activemq.filter.FilterException;
import org.apache.activemq.filter.MessageEvaluationContext;


/**
 * Represents a filter which only operates on Destinations
 * 
 * @version $Revision: 1.3 $
 */
public abstract class DestinationFilter implements BooleanExpression {

    public static final String ANY_DESCENDENT = ">";
    public static final String ANY_CHILD = "*";
    
	public boolean matches(MessageEvaluationContext message) throws FilterException {
		Destination destination = message.getDestination();
		return matches(destination);
	}
	public Object evaluate(MessageEvaluationContext message) throws FilterException {
		return matches(message) ? Boolean.TRUE : Boolean.FALSE;
	}
	
    public abstract boolean matches(Destination destination);

    public static DestinationFilter parseFilter(Destination destination) {
    	Collection<Destination> destinations = destination.getDestinations();
        if (destinations!=null) {
            return new CompositeDestinationFilter(destination);
        }
        String[] paths = DestinationPath.getDestinationPaths(destination);
        int idx = paths.length - 1;
        if (idx >= 0) {
            String lastPath = paths[idx];
            if (lastPath.equals(ANY_DESCENDENT)) {
                return new PrefixDestinationFilter(paths);
            } else {
                while (idx >= 0) {
                    lastPath = paths[idx--];
                    if (lastPath.equals(ANY_CHILD)) {
                        return new WildcardDestinationFilter(paths);
                    }
                }
            }
        }

        // if none of the paths contain a wildcard then use equality
        return new SimpleDestinationFilter(destination);
    }
}
