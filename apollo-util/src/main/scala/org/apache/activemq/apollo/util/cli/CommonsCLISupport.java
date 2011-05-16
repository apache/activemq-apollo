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

package org.apache.activemq.apollo.util.cli;

import java.util.ArrayList;
import java.util.HashSet;

import org.apache.activemq.apollo.util.IntrospectionSupport;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;

public class CommonsCLISupport {

    /**
     */
    static public String[] setOptions(Object target,  CommandLine cli) {
        Option[] options = cli.getOptions();
        for (Option option : options) {
            String name = option.getLongOpt();
            if( name==null )
                continue;

            String propName = convertOptionToPropertyName(name);


            String value = option.getValue();
            if( value!=null ) {
                Class<?> type = IntrospectionSupport.getPropertyType(target, propName);
                if( type.isArray() ) {
                    IntrospectionSupport.setProperty(target, propName, option.getValues());
                } else if( type.isAssignableFrom(ArrayList.class) ) {
                    IntrospectionSupport.setProperty(target, propName, new ArrayList(option.getValuesList()) );
                } else if( type.isAssignableFrom(HashSet.class) ) {
                    IntrospectionSupport.setProperty(target, propName, new HashSet(option.getValuesList()) );
                } else {
                    IntrospectionSupport.setProperty(target, propName, value);
                }
            } else {
                IntrospectionSupport.setProperty(target, propName, true);
            }
        }
        return cli.getArgs();
    }

    /**
     * converts strings like: test-enabled to testEnabled
     * @param name
     * @return
     */
    private static String convertOptionToPropertyName(String name) {
        String rc="";

        // Look for '-' and strip and then convert the subsequent char to uppercase
        int p = name.indexOf("-");
        while( p > 0 ) {
            // strip
            rc += name.substring(0, p);
            name = name.substring(p+1);

            // can I convert the next char to upper?
            if( name.length() >0 ) {
                rc += name.substring(0,1).toUpperCase();
                name = name.substring(1);
            }

            p = name.indexOf("-");
        }
        return rc+name;
    }

}
