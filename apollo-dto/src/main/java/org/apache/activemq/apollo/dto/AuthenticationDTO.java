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
import javax.xml.bind.annotation.XmlElement;
import java.util.ArrayList;
import java.util.List;

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class AuthenticationDTO {

    @XmlAttribute
    public Boolean enabled;

    @XmlAttribute
    public String domain;

    /**
     * The class names for the types of principles that
     * the acl lists check against.
     */
    @XmlElement(name="acl_principal_kind")
    public List<String> acl_principal_kinds = new ArrayList<String>();

    public List<String> acl_principal_kinds() {
        if( acl_principal_kinds.isEmpty() ) {
            ArrayList<String> rc = new ArrayList<String>();
            rc.add("org.apache.activemq.jaas.GroupPrincipal");
            return rc;
        }
        return acl_principal_kinds;
    }

    /**
     * The class names for the types of principles that
     * the user name is extracted from.
     */
    @XmlElement(name="user_principal_kind")
    public List<String> user_principal_kinds = new ArrayList<String>();

    public List<String> user_principal_kinds() {
        if( user_principal_kinds.isEmpty() ) {
            ArrayList<String> rc = new ArrayList<String>();
            rc.add("org.apache.activemq.jaas.UserPrincipal");
            return rc;
        }
        return user_principal_kinds;
    }
}
