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
package org.apache.activemq.filter;

import org.apache.activemq.apollo.broker.Destination;
import org.springframework.beans.factory.InitializingBean;

/**
 * A base class for entry objects used to construct a destination based policy
 * map.
 * 
 * @version $Revision: 1.1 $
 */
public abstract class DestinationMapEntry implements InitializingBean, Comparable<DestinationMapEntry> {

    private Destination destination;

    public int compareTo(DestinationMapEntry that) {
    	if( that == null )
    		return 1;
        return compare(destination, that.destination);
    }
    
    public static int compare(Destination destination, Destination destination2) {
        if (destination == destination2) {
            return 0;
        }
        if (destination == null) {
            return -1;
        } else if (destination2 == null) {
            return 1;
        } else {
        	int rc = destination.getDomain().compareTo(destination2.getDomain());
        	if( rc == 0 ) {
        		rc = destination.getName().compareTo(destination2.getName());;
        	}
        	return rc;
        }
    }
    

//    /**
//     * A helper method to set the destination from a configuration file
//     */
//    public void setQueue(String name) {
//        setDestination(new ActiveMQQueue(name));
//    }
//
//    /**
//     * A helper method to set the destination from a configuration file
//     */
//    public void setTopic(String name) {
//        setDestination(new ActiveMQTopic(name));
//    }

    public Destination getDestination() {
        return destination;
    }

    public void setDestination(Destination destination) {
        this.destination = destination;
    }

    public void afterPropertiesSet() throws Exception {
        if (destination == null) {
            throw new IllegalArgumentException("You must specify the 'destination' property");
        }
    }

}
