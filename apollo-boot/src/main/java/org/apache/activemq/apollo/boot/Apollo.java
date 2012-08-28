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
package org.apache.activemq.apollo.boot;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;

/**
 * <p>
 * A main class which setups up a classpath and then passes
 * execution off to another main class
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class Apollo {

   public static void main(String[] args) throws Throwable, NoSuchMethodException, IllegalAccessException {
       LinkedList<String> argList = new LinkedList<String>(Arrays.asList(args));
       if( argList.isEmpty() ) {
           System.err.println("Usage "+Apollo.class.getName()+" <bootdir>(;<bootdir>)* <main-class> <arg>*");
           System.err.println();
           System.err.println("Mising <bootdir>");
           System.exit(2);
       }
       String[] bootdirs = resolveBootDirs(argList);

       if( argList.isEmpty() ) {
           System.err.println("Usage "+Apollo.class.getName()+" <bootdir>(;<bootdir>)* <main-class> <arg>*");
           System.err.println();
           System.err.println("Mising <main-class>");
           System.exit(2);
       }
       String mainClass = argList.removeFirst();
       String[] mainArgs = argList.toArray(new String[argList.size()]);

       ArrayList<URL> urls = new ArrayList<URL>();

       for(String dir:bootdirs) {
           dir = dir.trim();
           if( dir.isEmpty() ) {
               continue;
           }

           File bootdir = new File(dir);
           if( bootdir.isDirectory() ) {

               // Find the jar files in the directory..
               ArrayList<File> files = new ArrayList<File>();
               for( File f : bootdir.listFiles() ) {
                   if( f.getName().endsWith(".jar") || f.getName().endsWith(".zip") ) {
                       files.add(f);
                   }
               }
               // Sort the list by file name..
               Collections.sort(files, new Comparator<File>() {
                   public int compare(File file, File file1) {
                       return file.getName().compareTo(file1.getName());
                   }
               });

               for( File f : files ) {
                   add(urls, f);
               }

           } else if( bootdir.isFile() ) {
               add(urls, bootdir);
           }
       }

       String tempdir = System.getProperty("apollo.tempdir");
       if( tempdir == null ) {
           String base = System.getProperty("apollo.base");
           if( base !=null ) {
               tempdir = new File(base, "tmp").getCanonicalPath();
           }
       }

       if( tempdir != null ) {
           System.setProperty("java.io.tmpdir", tempdir);
       }

       // Now setup our classloader..
       URLClassLoader loader = new URLClassLoader(urls.toArray(new URL[urls.size()]));
       Class<?> clazz = loader.loadClass(mainClass);
       Method method = clazz.getMethod("main", args.getClass());
       try {
           method.invoke(null, (Object)mainArgs);
       } catch (InvocationTargetException e) {
           throw e.getTargetException();
       }

   }

   protected static String[] resolveBootDirs(LinkedList<String> argList) {
       return argList.removeFirst().split(";");
   }

   static private void add(ArrayList<URL> urls, File file) {
       try {
           urls.add(file.toURI().toURL());
       } catch (MalformedURLException e) {
           e.printStackTrace();
       }
   }

}
