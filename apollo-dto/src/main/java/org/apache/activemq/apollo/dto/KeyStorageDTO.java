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
import java.io.File;

/**
 *  Used to specify the location of key store and key store access
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@XmlRootElement(name="key_storage")
@XmlAccessorType(XmlAccessType.FIELD)
@JsonIgnoreProperties(ignoreUnknown = true)
public class KeyStorageDTO {

    /**
     * Path to where the key store is located.
     */
    @XmlAttribute
    public File file;

    /**
     * The key store password.
     */
    @XmlAttribute
    public String password;

    /**
     * The alias of the key to use.  Defaults to the first key found.
     */
    @XmlAttribute(name="key_alias")
    public String key_alias;

    /**
     * The password to the keys in the key store.
     */
    @XmlAttribute(name="key_password")
    public String key_password;

    /**
     * The type of key store.  If not set, defaults to JKS.
     */
    @XmlAttribute(name="store_type")
    public String store_type;

    /**
     * The trust management algorithm.  If not set, defaults to SunX509.
     */
    @XmlAttribute(name="trust_algorithm")
    public String trust_algorithm;

    /**
     * The key management algorithm.  If not set, defaults to SunX509.
     */
    @XmlAttribute(name="key_algorithm")
    public String key_algorithm;


}
