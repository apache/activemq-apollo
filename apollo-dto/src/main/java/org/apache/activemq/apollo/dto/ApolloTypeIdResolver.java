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

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.jsontype.TypeIdResolver;
import com.fasterxml.jackson.databind.type.TypeFactory;
import org.apache.activemq.apollo.util.DtoModule$;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

/**
 */
public class ApolloTypeIdResolver implements TypeIdResolver {

    protected final HashMap<Class<?>, String> typeToId = new HashMap<Class<?>, String>();
    protected final HashMap<String, JavaType> idToType = new HashMap<String, JavaType>();

    private JavaType baseType;
    public void init(JavaType baseType) {
        this.baseType = baseType;
        ArrayList<Class<?>> classes = new ArrayList<Class<?>>();
        classes.add(baseType.getRawClass());
        classes.addAll(Arrays.asList(DtoModule$.MODULE$.extension_classes()));
        for ( Class<?> c : classes) {
            if( baseType.getRawClass().isAssignableFrom(c) ) {
                JsonTypeName jsonAnnoation = c.getAnnotation(JsonTypeName.class);
                if(jsonAnnoation!=null && jsonAnnoation.value()!=null) {
                    typeToId.put(c, jsonAnnoation.value());
                    idToType.put(jsonAnnoation.value(), TypeFactory.defaultInstance().constructSpecializedType(baseType, c));
                    idToType.put(c.getName(), TypeFactory.defaultInstance().constructSpecializedType(baseType, c));
                } else {
                    XmlRootElement xmlAnnoation = c.getAnnotation(XmlRootElement.class);
                    if(xmlAnnoation!=null && xmlAnnoation.name()!=null) {
                        typeToId.put(c, xmlAnnoation.name());
                        idToType.put(xmlAnnoation.name(), TypeFactory.defaultInstance().constructSpecializedType(baseType, c));
                        idToType.put(c.getName(), TypeFactory.defaultInstance().constructSpecializedType(baseType, c));
                    }
                }
            }
        }
    }

    public JsonTypeInfo.Id getMechanism() {
        return JsonTypeInfo.Id.CUSTOM;  
    }

    public String idFromValue(Object value) {
        return idFromValueAndType(value, value.getClass());
    }

    public String idFromValueAndType(Object value, Class<?> aClass) {
        String rc = typeToId.get(aClass);
        if(rc==null)
            throw new IllegalArgumentException("Invalid sub type: "+aClass+", of base type: "+baseType.getRawClass());
        return rc;
    }

    @Override
    public String idFromBaseType() {
        return idFromValueAndType(null, baseType.getRawClass());
    }

    public JavaType typeFromId(String id) {
        JavaType rc = idToType.get(id);
        if(rc==null)
            throw new IllegalArgumentException("Invalid type id '"+id);
        return rc;
    }
}
