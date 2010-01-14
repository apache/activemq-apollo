package org.apache.activemq.amqp.generator;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.StringTokenizer;

public class Utils {

    public static final String JAVA_TAB = "    ";
    public static final String NL = "\n";

    public static final String toJavaName(String name) {
        StringTokenizer tok = new StringTokenizer(name.trim(), "- ");
        String javaName = "";
        int i = 0;
        while (tok.hasMoreElements()) {
            String token = tok.nextToken();
            if (i > 0) {
                javaName += token.substring(0, 1).toUpperCase();
                javaName += token.substring(1);
            } else {
                javaName += token;
            }
            i++;
        }
        return javaName;
    }

    public static final String toJavaConstant(String name) {
        StringTokenizer tok = new StringTokenizer(name.trim(), "- ");
        String javaName = "";
        int i = 0;
        while (tok.hasMoreElements()) {
            String token = tok.nextToken().toUpperCase();
            if (i > 0) {
                javaName += "_";
            }
            javaName += token;
            i++;
        }
        return javaName;
    }

    public static final String capFirst(String toCap) {
        String ret = "";
        ret += toCap.substring(0, 1).toUpperCase();
        ret += toCap.substring(1);
        return ret;
    }

    public static final String tab(int num) {
        String ret = "";
        for (int i = 0; i < num; i++) {
            ret += JAVA_TAB;
        }
        return ret;
    }

    public static final String padHex(String hex, int count) {
        while (hex.length() < count) {
            hex = "0" + hex;
        }
        return hex;
    }
    
    public static void writeJavaCopyWrite(BufferedWriter writer) throws IOException {
        
        writeJavaComment(writer, 0, 
                "Licensed to the Apache Software Foundation (ASF) under one or more",
                "contributor license agreements.  See the NOTICE file distributed with",
                "his work for additional information regarding copyright ownership.",
                "The ASF licenses this file to You under the Apache License, Version 2.0",
                "(the \"License\"); you may not use this file except in compliance with",
                "the License.  You may obtain a copy of the License at",
                "",
                "     http://www.apache.org/licenses/LICENSE-2.0",
                "",
                "Unless required by applicable law or agreed to in writing, software",
                "distributed under the License is distributed on an \"AS IS\" BASIS,",
                "WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.",
                "See the License for the specific language governing permissions and",
                "limitations under the License.");
    }
    
    public static void writeJavaComment(BufferedWriter writer, int indent, String ... commentLines) throws IOException
    {
        if(commentLines == null)
        {
            return;
        }
        
        writer.write(tab(indent) + "/**");
        writer.newLine();
        for(String s : commentLines)
        {
            writer.write(tab(indent) + " * " + s);
            writer.newLine();
        }
        writer.write(tab(indent) + " */");
        writer.newLine();
    }

}
