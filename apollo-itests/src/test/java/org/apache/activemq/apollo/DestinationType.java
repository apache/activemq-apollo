package org.apache.activemq.apollo;

import javax.jms.Destination;
import javax.jms.*;


/**
* @author <a href="http://hiramchirino.com">Hiram Chirino</a>
*/
public enum DestinationType {
    QUEUE_TYPE,
    TOPIC_TYPE,
    TEMP_QUEUE_TYPE,
    TEMP_TOPIC_TYPE;

    public static DestinationType of(Destination d) {
        if( d instanceof Queue) {
            if( d instanceof TemporaryQueue ) {
                return TEMP_QUEUE_TYPE;
            } else {
                return QUEUE_TYPE;
            }
        }
        if( d instanceof Topic) {
            if( d instanceof TemporaryTopic ) {
                return TEMP_TOPIC_TYPE;
            } else {
                return TOPIC_TYPE;
            }
        }
        return null;
    }
}
