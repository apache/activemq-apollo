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

import org.apache.activemq.apollo.util.*;
import org.fusesource.hawtbuf.Buffer;
import scala.*;
import scala.collection.JavaConversions$;
import scala.collection.Seq;
import scala.collection.immutable.List;
import scala.runtime.BoxedUnit;

import java.lang.Boolean;
import java.lang.Long;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class Scala2Java {
    private static Scala2JavaHelper$ helper = Scala2JavaHelper$.MODULE$;


    public static int get(Integer value, int defaultValue) {
        if( value!=null ) {
            return value.intValue();
        } else {
            return defaultValue;
        }
    }
    public static long get(Long value, long defaultValue) {
        if( value!=null ) {
            return value.longValue();
        } else {
            return defaultValue;
        }
    }
    public static boolean get(Boolean value, boolean defaultValue) {
        if( value!=null ) {
            return value.booleanValue();
        } else {
            return defaultValue;
        }
    }

    static final Function1<Object,BoxedUnit> NOOP_FN1 = helper.toScala(new UnitFn1<Object>() {
        @Override
        public void call(Object v1) {
        }
    });

    public static String toString(Object o) {
        return o == null ? null : o.toString();
    }

    public static <T1> Function1<T1, BoxedUnit> noopFn1() {
        return (Function1<T1, BoxedUnit>) NOOP_FN1;
    }

    public static <R> Function0<R> toScala(Fn0<R> func) {
        if( func == null ) {
            return null;
        }
        return helper.toScala(func);
    }

    public static <T1, R> Function1<T1, R> toScala(Fn1<T1, R> func) {
        if( func == null ) {
            return null;
        }
        return helper.toScala(func);
    }

    public static <T1, T2, R> Function2<T1, T2, R> toScala(Fn2<T1, T2, R> func) {
        if( func == null ) {
            return null;
        }
        return helper.toScala(func);
    }

    public static <T> Option<T> none() {
        return helper.none();
    }

    public static <T> Option<T> some(T t) {
        return helper.some(t);
    }

    public static <T> T head(Iterable<T> i) {
        Iterator<T> iterator = i.iterator();
        if( iterator.hasNext() ) {
            return iterator.next();
        } else {
            return null;
        }
    }

    public static <T,R> ArrayList<R> map(Collection<T> values, Fn1<T, R> func) {
        ArrayList rc = new ArrayList(values.size());
        for( T t: values) {
            rc.add(func.apply(t));
        }
        return rc;
    }

    public static <T,R> ArrayList<R> flatMap(Collection<T> values, Fn1<T, Option<R>> func) {
        ArrayList rc = new ArrayList(values.size());
        for( T t: values) {
            Option<R> opt = func.apply(t);
            if( opt.isDefined() ) {
                rc.add(opt.get());
            }
        }
        return rc;
    }

    public static List<String> toList(String ... s) {
        return helper.toList(s);
    }

    public static <T> Iterable<T> toIterable(Seq<T> entries) {
        return JavaConversions$.MODULE$.asJavaIterable(entries);
    }

    public static class Logger {
        final Log log;

        public Logger(Log log) {
            this.log = log;
        }

        public <T> void trace(String message, Object ...args) {
            helper.trace(log, message, args);
        }
        public <T> void debug(String message, Object ...args) {
            helper.debug(log, message, args);
        }
        public <T> void info(String message, Object ...args) {
            helper.info(log, message, args);
        }
        public <T> void warn(String message, Object ...args) {
            helper.warn(log, message, args);
        }
        public <T> void error(String message, Object ...args) {
            helper.error(log, message, args);
        }

        public <T> void trace(Throwable e, String message, Object ...args) {
            helper.trace(log, e, message, args);
        }
        public <T> void debug(Throwable e, String message, Object ...args) {
            helper.debug(log, e, message, args);
        }
        public <T> void info(Throwable e, String message, Object ...args) {
            helper.info(log, e, message, args);
        }
        public <T> void warn(Throwable e, String message, Object ...args) {
            helper.warn(log, e, message, args);
        }
        public <T> void error(Throwable e, String message, Object ...args) {
            helper.error(log, e, message, args);
        }
    }
}
