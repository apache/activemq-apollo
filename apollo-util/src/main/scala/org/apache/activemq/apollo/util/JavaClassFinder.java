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

import scala.collection.immutable.Nil;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.*;

/**
 * <p>
 * Used to discover classes using the META-INF discovery trick.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class JavaClassFinder<T> {
    private final String path;
    private final ClassLoader[] loaders;

    public JavaClassFinder(String path) {
        this(path, new ClassLoader[]{Thread.currentThread().getContextClassLoader()});
    }

    public JavaClassFinder(String path, ClassLoader[] loaders) {
        this.path = path;
        this.loaders = loaders;
    }

    public List<Class<T>> find() {

        HashSet<Class<T>> classes = new HashSet<Class<T>>();
        for (ClassLoader loader : loaders) {

            try {
                Enumeration<URL> resources = loader.getResources(path);
                HashSet<String> classNames = new HashSet<String>();

                while (resources.hasMoreElements()) {
                    URL url = resources.nextElement();
                    Properties p = loadProperties(url.openStream());
                    Enumeration<Object> keys = p.keys();
                    while (keys.hasMoreElements()) {
                        classNames.add((String) keys.nextElement());
                    }
                }
                
                for (String name : classNames) {
                    try {
                        classes.add((Class<T>) loader.loadClass(name));
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return new ArrayList<Class<T>>(classes);
    }

    public List<T> new_instances() {
        ArrayList<T> t = new ArrayList<T>();
        for (Class<T> clazz : find()) {
            try {
                t.add(clazz.newInstance());
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }
        return t;
    }

    private Properties loadProperties(InputStream is) {
        if (is == null) {
            return null;
        }
        try {
            Properties p = new Properties();
            p.load(is);
            return p;
        } catch (Exception e) {
            return null;
        } finally {
            try {
                is.close();
            } catch (Throwable e) {
            }
        }
    }
}