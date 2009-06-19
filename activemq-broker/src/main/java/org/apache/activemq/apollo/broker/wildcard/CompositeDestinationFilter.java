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
import org.apache.activemq.filter.FilterException;


/**
 * A {@link DestinationFilter} used for composite destinations
 * 
 * @version $Revision: 1.3 $
 */
public class CompositeDestinationFilter extends DestinationFilter {

    private DestinationFilter filters[];

    public CompositeDestinationFilter(Destination destination) {
    	Collection<Destination> destinations = destination.getDestinations();
        filters = new DestinationFilter[destinations.size()];
        int i=0;
    	for (Destination childDestination : destinations) {
            filters[i++] = DestinationFilter.parseFilter(childDestination);
		}
    }

    public boolean matches(Destination destination) throws FilterException {
        for (int i = 0; i < filters.length; i++) {
            if (filters[i].matches(destination)) {
                return true;
            }
        }
        return false;
    }

    public boolean isWildcard() {
        return true;
    }
}
