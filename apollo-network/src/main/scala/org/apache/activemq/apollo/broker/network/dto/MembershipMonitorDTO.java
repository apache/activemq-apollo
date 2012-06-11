package org.apache.activemq.apollo.broker.network.dto;

import org.apache.activemq.apollo.dto.ApolloTypeIdResolver;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonTypeInfo;
import org.codehaus.jackson.map.annotate.JsonTypeIdResolver;

import javax.xml.bind.annotation.XmlType;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@XmlType(name = "membership_monitor_type")
@JsonTypeInfo(use=JsonTypeInfo.Id.CUSTOM, include=JsonTypeInfo.As.PROPERTY, property="@class")
@JsonTypeIdResolver(ApolloTypeIdResolver.class)
@JsonIgnoreProperties(ignoreUnknown = true)
abstract public class MembershipMonitorDTO {

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MembershipMonitorDTO)) return false;
        return true;
    }

    @Override
    public int hashCode() {
        return 0;
    }
}