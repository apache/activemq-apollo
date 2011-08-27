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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@XmlRootElement(name="access_rule")
@XmlAccessorType(XmlAccessType.FIELD)
public class AccessRuleDTO {

    /**
     * Is this a negative rule which denies access.  If not set, defaults to false.
     */
    @XmlAttribute
    public Boolean deny;

    /**
     * The class name of the JAAS principle that this rule will mach against.  If not set
     * the this defaults to the default principal kinds configured on the broker or virtual host.
     * If set to "*" then it matches all principal classes.
     */
    @XmlAttribute(name = "principal_kind")
    public String principal_kind;

    /**
     * The principal which we are matching against.  If set to "+" then it matches all principals
     * but requires at at least one.  If set to "*" the it matches all principals and even matches
     * the case where there are no principals associated with the subject.
     *
     * Defaults to "+" if not set.
     */
    @XmlAttribute
    public String principal;

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
     * The kind of broker resource which matches this rule.  You can use "*" to match all types.  If not set
     * it defaults to "*"
     */
    @XmlAttribute
    public String kind;

    /**
     * The identifier of the resource which matches this rule.  You can use "*" to match all resources.  If not set
     * it defaults to "*"
     */
    @XmlAttribute
    public String id;

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

        if (action != null ? !action.equals(that.action) : that.action != null) return false;
        if (deny != null ? !deny.equals(that.deny) : that.deny != null) return false;
        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        if (id_regex != null ? !id_regex.equals(that.id_regex) : that.id_regex != null) return false;
        if (kind != null ? !kind.equals(that.kind) : that.kind != null) return false;
        if (principal != null ? !principal.equals(that.principal) : that.principal != null) return false;
        if (principal_kind != null ? !principal_kind.equals(that.principal_kind) : that.principal_kind != null)
            return false;
        if (separator != null ? !separator.equals(that.separator) : that.separator != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = deny != null ? deny.hashCode() : 0;
        result = 31 * result + (principal_kind != null ? principal_kind.hashCode() : 0);
        result = 31 * result + (principal != null ? principal.hashCode() : 0);
        result = 31 * result + (separator != null ? separator.hashCode() : 0);
        result = 31 * result + (action != null ? action.hashCode() : 0);
        result = 31 * result + (kind != null ? kind.hashCode() : 0);
        result = 31 * result + (id != null ? id.hashCode() : 0);
        result = 31 * result + (id_regex != null ? id_regex.hashCode() : 0);
        return result;
    }
}
