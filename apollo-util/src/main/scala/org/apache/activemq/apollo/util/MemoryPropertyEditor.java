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
package org.apache.activemq.apollo.util;

import java.beans.PropertyEditorSupport;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Converts string values like "20 Mib", "1024kb", and "1g" to long values in bytes.
 */
public class MemoryPropertyEditor extends PropertyEditorSupport {

    private static final Pattern BYTE_PATTERN = Pattern.compile("^\\s*(-?\\d+)\\s*b?\\s*$", Pattern.CASE_INSENSITIVE);

    private static final Pattern KIB_PATTERN = Pattern.compile("^\\s*(-?\\d+)\\s*k(ib)?\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern MIB_PATTERN = Pattern.compile("^\\s*(-?\\d+)\\s*m(ib)?\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern GIB_PATTERN = Pattern.compile("^\\s*(-?\\d+)\\s*g(ib)?\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern TIB_PATTERN = Pattern.compile("^\\s*(-?\\d+)\\s*t(ib)?\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern PIB_PATTERN = Pattern.compile("^\\s*(-?\\d+)\\s*p(ib)?\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern EIB_PATTERN = Pattern.compile("^\\s*(-?\\d+)\\s*e(ib)?\\s*$", Pattern.CASE_INSENSITIVE);

    private static final Pattern KB_PATTERN = Pattern.compile("^\\s*(-?\\d+)\\s*kb?\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern MB_PATTERN = Pattern.compile("^\\s*(-?\\d+)\\s*mb?\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern GB_PATTERN = Pattern.compile("^\\s*(-?\\d+)\\s*gb?\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern TB_PATTERN = Pattern.compile("^\\s*(-?\\d+)\\s*tb?\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern PB_PATTERN = Pattern.compile("^\\s*(-?\\d+)\\s*pb?\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern EB_PATTERN = Pattern.compile("^\\s*(-?\\d+)\\s*eb?\\s*$", Pattern.CASE_INSENSITIVE);

    public void setAsText(String text) throws IllegalArgumentException {

        Matcher m = BYTE_PATTERN.matcher(text);
        if (m.matches()) {
            setValue(Long.valueOf(Long.parseLong(m.group(1))));
            return;
        }
        m = KIB_PATTERN.matcher(text);
        if (m.matches()) {
            setValue(Long.valueOf(Long.parseLong(m.group(1)) * 1024));
            return;
        }
        m = MIB_PATTERN.matcher(text);
        if (m.matches()) {
            setValue(Long.valueOf(Long.parseLong(m.group(1)) * 1024 * 1024));
            return;
        }
        m = GIB_PATTERN.matcher(text);
        if (m.matches()) {
            setValue(Long.valueOf(Long.parseLong(m.group(1)) * 1024 * 1024 * 1024));
            return;
        }
        m = TIB_PATTERN.matcher(text);
        if (m.matches()) {
            setValue(Long.valueOf(Long.parseLong(m.group(1)) * 1024 * 1024 * 1024 * 1024));
            return;
        }
        m = PIB_PATTERN.matcher(text);
        if (m.matches()) {
            setValue(Long.valueOf(Long.parseLong(m.group(1)) * 1024 * 1024 * 1024 * 1024 * 1024));
            return;
        }
        m = EIB_PATTERN.matcher(text);
        if (m.matches()) {
            setValue(Long.valueOf(Long.parseLong(m.group(1)) * 1024 * 1024 * 1024 * 1024 * 1024 * 1024));
            return;
        }

        m = KB_PATTERN.matcher(text);
        if (m.matches()) {
            setValue(Long.valueOf(Long.parseLong(m.group(1)) * 1000));
            return;
        }
        m = MB_PATTERN.matcher(text);
        if (m.matches()) {
            setValue(Long.valueOf(Long.parseLong(m.group(1)) * 1000 * 1000));
            return;
        }
        m = GB_PATTERN.matcher(text);
        if (m.matches()) {
            setValue(Long.valueOf(Long.parseLong(m.group(1)) * 1000 * 1000 * 1000));
            return;
        }
        m = TB_PATTERN.matcher(text);
        if (m.matches()) {
            setValue(Long.valueOf(Long.parseLong(m.group(1)) * 1000 * 1000 * 1000 * 1000));
            return;
        }
        m = PB_PATTERN.matcher(text);
        if (m.matches()) {
            setValue(Long.valueOf(Long.parseLong(m.group(1)) * 1000 * 1000 * 1000 * 1000 * 1000));
            return;
        }
        m = EB_PATTERN.matcher(text);
        if (m.matches()) {
            setValue(Long.valueOf(Long.parseLong(m.group(1)) * 1000 * 1000 * 1000 * 1000 * 1000 * 1000));
            return;
        }

        throw new IllegalArgumentException("Could convert not to a memory size: " + text);
    }

    public String getAsText() {
        Long value = (Long)getValue();
        if( value == null ) 
            return "";
        long v = value.longValue();
        
        if( (v % 1024) != 0) {
            if( (v % 1000) != 0) {
                return Long.toString(v);
            } else {
                v = v/1000;
                if( (v % 1000) != 0) {
                    return v+"kB";
                } else {
                    v = v/1000;
                    if( (v % 1000) != 0) {
                        return v+"MB";
                    } else {
                        v = v/1000;
                        if( (v % 1000) != 0) {
                            return v+"GB";
                        } else {
                            v = v/1000;
                            if( (v % 1000) != 0) {
                                return v+"TB";
                            } else {
                                v = v/1000;
                                if( (v % 1000) != 0) {
                                    return v+"PB";
                                } else {
                                    v = v/1000;
                                    return v+"EB";
                                }
                            }
                        }
                    }
                }
            }
        } else {
            v = v/1024;
            if( (v % 1024) != 0) {
                return v+"KiB";
            } else {
                v = v/1024;
                if( (v % 1024) != 0) {
                    return v+"MiB";
                } else {
                    v = v/1024;
                    if( (v % 1024) != 0) {
                        return v+"GiB";
                    } else {
                        v = v/1024;
                        if( (v % 1024) != 0) {
                            return v+"TiB";
                        } else {
                            v = v/1024;
                            if( (v % 1024) != 0) {
                                return v+"PiB";
                            } else {
                                v = v/1024;
                                return v+"EiB";
                            }
                        }
                    }
                }
            }
        }
    }

    public static long parse(String value) {
        MemoryPropertyEditor pe = new MemoryPropertyEditor();
        pe.setAsText(value);
        return (Long)pe.getValue();
    }
    
    public static String format(long value) {
        MemoryPropertyEditor pe = new MemoryPropertyEditor();
        pe.setValue(value);
        return pe.getAsText();
    }
}
