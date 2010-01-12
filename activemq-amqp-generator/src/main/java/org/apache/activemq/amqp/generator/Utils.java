package org.apache.activemq.amqp.generator;

import java.util.StringTokenizer;

public class Utils {

    public static final String JAVA_TAB = "    ";
    public static final String NL = "\n";

    public static final String toJavaName(String name) {
        StringTokenizer tok = new StringTokenizer(name, "-");
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
        StringTokenizer tok = new StringTokenizer(name, "-");
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

}
