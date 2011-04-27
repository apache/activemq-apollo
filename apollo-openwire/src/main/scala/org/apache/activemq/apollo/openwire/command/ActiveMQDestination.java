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
package org.apache.activemq.apollo.openwire.command;

import java.util.ArrayList;

import org.apache.activemq.apollo.broker.DestinationParser;
import org.apache.activemq.apollo.broker.LocalRouter;
import org.apache.activemq.apollo.dto.DestinationDTO;
import org.apache.activemq.apollo.util.path.Path;
import org.fusesource.hawtbuf.AsciiBuffer;

/**
 * @openwire:marshaller
 * @version $Revision: 1.10 $
 */
abstract public class ActiveMQDestination implements DataStructure, Comparable {

    public static final String PATH_SEPERATOR = ".";
    public static final char COMPOSITE_SEPERATOR = ',';


    public static final DestinationParser PARSER = new DestinationParser();
    static {
        PARSER.path_seperator = new AsciiBuffer(".");
        PARSER.any_child_wildcard = new AsciiBuffer("*");
        PARSER.any_descendant_wildcard = new AsciiBuffer(">");
    }

    public static final byte QUEUE_TYPE = 0x01;
    public static final byte TOPIC_TYPE = 0x02;
    public static final byte TEMP_MASK = 0x04;
    public static final byte TEMP_TOPIC_TYPE = TOPIC_TYPE | TEMP_MASK;
    public static final byte TEMP_QUEUE_TYPE = QUEUE_TYPE | TEMP_MASK;

    public static final String QUEUE_QUALIFIED_PREFIX = "queue://";
    public static final String TOPIC_QUALIFIED_PREFIX = "topic://";
    public static final String TEMP_QUEUE_QUALIFED_PREFIX = "temp-queue://";
    public static final String TEMP_TOPIC_QUALIFED_PREFIX = "temp-topic://";

    public static final String TEMP_DESTINATION_NAME_PREFIX = "ID:";

    private static final long serialVersionUID = -3885260014960795889L;

    protected String physicalName;

    protected transient DestinationDTO[] destination;

    public ActiveMQDestination() {
    }

    protected ActiveMQDestination(String name) {
        setPhysicalName(name);
    }


    public DestinationDTO[] toDestination() {
        return destination;
    }

    // static helper methods for working with destinations
    // -------------------------------------------------------------------------
    public static ActiveMQDestination createDestination(String name, byte defaultType) {

        if (name.startsWith(QUEUE_QUALIFIED_PREFIX)) {
            return new ActiveMQQueue(name.substring(QUEUE_QUALIFIED_PREFIX.length()));
        } else if (name.startsWith(TOPIC_QUALIFIED_PREFIX)) {
            return new ActiveMQTopic(name.substring(TOPIC_QUALIFIED_PREFIX.length()));
        } else if (name.startsWith(TEMP_QUEUE_QUALIFED_PREFIX)) {
            return new ActiveMQTempQueue(name.substring(TEMP_QUEUE_QUALIFED_PREFIX.length()));
        } else if (name.startsWith(TEMP_TOPIC_QUALIFED_PREFIX)) {
            return new ActiveMQTempTopic(name.substring(TEMP_TOPIC_QUALIFED_PREFIX.length()));
        }

        switch (defaultType) {
        case QUEUE_TYPE:
            return new ActiveMQQueue(name);
        case TOPIC_TYPE:
            return new ActiveMQTopic(name);
        case TEMP_QUEUE_TYPE:
            return new ActiveMQTempQueue(name);
        case TEMP_TOPIC_TYPE:
            return new ActiveMQTempTopic(name);
        default:
            throw new IllegalArgumentException("Invalid default destination type: " + defaultType);
        }
    }

    public static int compare(ActiveMQDestination destination, ActiveMQDestination destination2) {
        if (destination == destination2) {
            return 0;
        }
        if (destination == null) {
            return -1;
        } else if (destination2 == null) {
            return 1;
        } else {
            if (destination.isQueue() == destination2.isQueue()) {
                return destination.getPhysicalName().compareTo(destination2.getPhysicalName());
            } else {
                return destination.isQueue() ? -1 : 1;
            }
        }
    }

    public int compareTo(Object that) {
        if (that instanceof ActiveMQDestination) {
            return compare(this, (ActiveMQDestination)that);
        }
        if (that == null) {
            return 1;
        } else {
            return getClass().getName().compareTo(that.getClass().getName());
        }
    }


    protected abstract String getQualifiedPrefix();

    /**
     * @openwire:property version=1
     */
    public String getPhysicalName() {
        return physicalName;
    }

    DestinationDTO[] create_destination(AsciiBuffer domain, Path path) {
        return DestinationParser.create_destination(domain, DestinationParser.encode_path(path));
    }

    public void setPhysicalName(String value) {
        physicalName = value;
        String[] composites = value.split(",");
        if(composites.length == 1) {
            Path path = PARSER.parsePath(new AsciiBuffer(composites[0]));
            switch(getDestinationType()) {
                case QUEUE_TYPE:
                    destination = create_destination(LocalRouter.QUEUE_DOMAIN(), path);
                    break;
                case TOPIC_TYPE:
                    destination = create_destination(LocalRouter.TOPIC_DOMAIN(), path);
                    break;
                case TEMP_QUEUE_TYPE:
                    destination = create_destination(LocalRouter.TEMP_QUEUE_DOMAIN(), path);
                    break;
                case TEMP_TOPIC_TYPE:
                    destination = create_destination(LocalRouter.TEMP_TOPIC_DOMAIN(), path);
                    break;
            }
        } else {
            ArrayList<DestinationDTO> l = new ArrayList<DestinationDTO>();
            for( String c:composites ) {
                l.add(createDestination(c).destination[0]);
            }
            destination = l.toArray(new DestinationDTO[l.size()]);
        }

    }

    public ActiveMQDestination createDestination(String name) {
        return createDestination(name, getDestinationType());
    }

    public abstract byte getDestinationType();

    public boolean isQueue() {
        return false;
    }

    public boolean isTopic() {
        return false;
    }

    public boolean isTemporary() {
        return false;
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ActiveMQDestination d = (ActiveMQDestination)o;
        return destination.equals(d.destination);
    }

    public int hashCode() {
        return destination.hashCode();
    }

    public String toString() {
        return destination.toString();
    }

    public String getDestinationTypeAsString() {
        switch (getDestinationType()) {
        case QUEUE_TYPE:
            return "Queue";
        case TOPIC_TYPE:
            return "Topic";
        case TEMP_QUEUE_TYPE:
            return "TempQueue";
        case TEMP_TOPIC_TYPE:
            return "TempTopic";
        default:
            throw new IllegalArgumentException("Invalid destination type: " + getDestinationType());
        }
    }

    public boolean isMarshallAware() {
        return false;
    }

    public boolean isComposite() {
        throw new UnsupportedOperationException();
    }

    public ActiveMQDestination[] getCompositeDestinations() {
        throw new UnsupportedOperationException();
    }
}
