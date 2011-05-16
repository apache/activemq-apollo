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

import org.apache.commons.cli.Option;

/**
 * a better version of org.apache.commons.cli.OptionBuilder
 * IDE provides nicer auto complete and less compiler warnings.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class OptionBuilder {

    private String id;
    private String name;
    private String description;
    private boolean required;
    private boolean optional;
    private int args =-1;
    private String arg;
    private Object type;
    private char sperator;

    public static OptionBuilder ob() {
        return new OptionBuilder();
    }

    public Option op() {
        Option option = new Option( id!=null ? id : " ", description );
        option.setLongOpt(name);
        option.setRequired( required );
        option.setOptionalArg(optional);
        option.setType( type );
        option.setValueSeparator(sperator);
        if( arg !=null && args==-1 ) {
            args=1;
        }
        option.setArgs(args);
        option.setArgName(arg);
        return option;
    }

    public OptionBuilder arg(String argName) {
        this.arg = argName;
        return this;
    }

    public OptionBuilder args(int args) {
        this.args = args;
        return this;
    }

    public OptionBuilder description(String description) {
        this.description = description;
        return this;
    }

    public OptionBuilder name(String lname) {
        this.name = lname;
        return this;
    }

    public OptionBuilder id(String name) {
        this.id = name;
        return this;
    }

    public OptionBuilder optional(boolean optional) {
        this.optional = optional;
        return this;
    }

    public OptionBuilder required(boolean required) {
        this.required = required;
        return this;
    }

    public OptionBuilder sperator(char sperator) {
        this.sperator = sperator;
        return this;
    }

    public OptionBuilder type(Object type) {
        this.type = type;
        return this;
    }
}