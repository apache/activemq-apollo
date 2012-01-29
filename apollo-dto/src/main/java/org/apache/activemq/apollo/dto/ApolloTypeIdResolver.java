package org.apache.activemq.apollo.dto;

import org.apache.activemq.apollo.util.DtoModule$;
import org.codehaus.jackson.annotate.JsonTypeInfo;
import org.codehaus.jackson.annotate.JsonTypeName;
import org.codehaus.jackson.map.jsontype.TypeIdResolver;
import org.codehaus.jackson.map.type.TypeFactory;
import org.codehaus.jackson.type.JavaType;

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
                    idToType.put(jsonAnnoation.value(), TypeFactory.specialize(baseType,  c));
                } else {
                    XmlRootElement xmlAnnoation = c.getAnnotation(XmlRootElement.class);
                    if(xmlAnnoation!=null && xmlAnnoation.name()!=null) {
                        typeToId.put(c, xmlAnnoation.name());
                        idToType.put(xmlAnnoation.name(), TypeFactory.specialize(baseType,  c));
                    }
                }
            }
        }
    }

    public JsonTypeInfo.Id getMechanism() {
        return JsonTypeInfo.Id.CUSTOM;  
    }

    public String idFromValue(Object value) {
        String rc = typeToId.get(value.getClass());
        if(rc==null)
            throw new IllegalArgumentException("Invalid sub type: "+value.getClass()+", of base type: "+baseType.getRawClass());
        return rc;
    }

    public JavaType typeFromId(String id) {
        JavaType rc = idToType.get(id);
        if(rc==null)
            throw new IllegalArgumentException("Invalid type id '"+id);
        return rc;
    }
}
