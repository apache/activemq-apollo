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

import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.ByteArrayOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class ProcessSupport {
    static private ThreadLocal<Integer> EXIT_CODE = new ThreadLocal<Integer> ();

    static public Buffer system(String...command) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            final Process process = Runtime.getRuntime().exec(command);
            Thread pumper = pump(process.getErrorStream(), System.err);
            IOHelper.copyInputStream(process.getInputStream(), baos);
            pumper.join();
            EXIT_CODE.set(process.waitFor());
        } catch (Exception e) {
            EXIT_CODE.set(-1);
        }
        return baos.toBuffer();
    }

    static public int lastExitCode() {
        final Integer code = EXIT_CODE.get();
        if( code == null )
            return 0;
        return code;
    }


    private static Thread pump(final InputStream in, final PrintStream out) {
        Thread rc = new Thread(){
            public void run() {
                try {
                    IOHelper.copyInputStream(in, out);
                } catch (IOException e) {
                }
            }
        };
        rc.start();
        return rc;
    }

}
