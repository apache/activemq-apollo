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
package org.apache.activemq.apollo.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * <p>
 * User authorization to broker resources is accomplished by configuring access control rules. The rules define
 * which principals are allowed or denied access to perform actions against server resources.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@XmlRootElement(name="access_rule")
@XmlAccessorType(XmlAccessType.FIELD)
@JsonIgnoreProperties(ignoreUnknown = true)
public class AccessRuleDTO {

    /**
     * A space separated list of class names of which will be matched against the principle type. If set to *
     * then it matches all principal classes. Defaults to the default principal kinds configured on the broker
     * or virtual host.
     */
    @XmlAttribute(name = "principal_kind")
    public String principal_kind;

    /**
     * The principal which are allowed access to the action.  If set to "+" then it matches all principals
     * but requires at at least one.  If set to "*" then it matches all principals and even matches
     * the case where there are no principals associated with the subject.
     */
    @XmlAttribute
    public String allow;

    /**
     * The principal which are denied access to the action  If set to "+" then it matches all principals
     * but requires at at least one.  If set to "*" then it matches all principals and even matches
     * the case where there are no principals associated with the subject.
     */
    @XmlAttribute
    public String deny;

    /**
     * If the separator is set, then the principal field will be interpreted as a list of
     * principles separated by the configured value.
     */
    @XmlAttribute
    public String separator;

    /**
     * The comma separated list of actions which match this rule.  Example 'create,destroy'.  You can use "*" to
     * match all actions.  Defaults to "*".
     */
    @XmlAttribute
    public String action;

    /**
     * A space separated list of broker resource types that will match this rule. You can use * to match all key.
     * Example values 'broker queue'. Defaults to *.
     */
    @XmlAttribute
    public String kind;

    /**
     * The identifier of the resource that will match this rule. You can use * to match all resources. If the
     * kind is set to queue or topic the your can use a destination wild card to match against the destination
     * id. Defaults to *.
     */
    @XmlAttribute
    public String id;

    /**
     * The id of the connector the user must be connected on for the
     * rule to match. You can use `*` to match all connectors. Defaults to `*`.
     */
    @XmlAttribute
    public String connector;

    /**
     * A regular expression used to match the id of the resource.
     */
    @XmlAttribute(name = "id_regex")
    public String id_regex;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AccessRuleDTO)) return false;

        AccessRuleDTO that = (AccessRuleDTO) o;

        if (action != null ? !action.equals(that.action) : that.action != null)
            return false;
        if (allow != null ? !allow.equals(that.allow) : that.allow != null)
            return false;
        if (deny != null ? !deny.equals(that.deny) : that.deny != null)
            return false;
        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        if (id_regex != null ? !id_regex.equals(that.id_regex) : that.id_regex != null)
            return false;
        if (kind != null ? !kind.equals(that.kind) : that.kind != null)
            return false;
        if (principal_kind != null ? !principal_kind.equals(that.principal_kind) : that.principal_kind != null)
            return false;
        if (separator != null ? !separator.equals(that.separator) : that.separator != null)
            return false;
        if (connector != null ? !connector.equals(that.connector) : that.connector != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = principal_kind != null ? principal_kind.hashCode() : 0;
        result = 31 * result + (allow != null ? allow.hashCode() : 0);
        result = 31 * result + (deny != null ? deny.hashCode() : 0);
        result = 31 * result + (separator != null ? separator.hashCode() : 0);
        result = 31 * result + (action != null ? action.hashCode() : 0);
        result = 31 * result + (kind != null ? kind.hashCode() : 0);
        result = 31 * result + (id != null ? id.hashCode() : 0);
        result = 31 * result + (id_regex != null ? id_regex.hashCode() : 0);
        result = 31 * result + (connector != null ? connector.hashCode() : 0);
        return result;
    }

    private static String attr(String name, Object value) {
        if(value!=null) {
            return " "+name+"='" + value +"'";
        } else {
            return "";
        }

    }
    @Override
    public String toString() {
        return "<access_rule" +
            attr("allow",allow)+
            attr("deny",deny)+
            attr("principal_kind",principal_kind)+
            attr("separator",separator)+
            attr("action",action)+
            attr("kind",kind)+
            attr("id",kind)+
            attr("id_regex",id_regex)+
            attr("connector",connector)+
            "/>";
    }
}
