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
package org.apache.activemq.syscall.jni;

import org.fusesource.hawtjni.runtime.JniArg;
import org.fusesource.hawtjni.runtime.JniClass;
import org.fusesource.hawtjni.runtime.JniMethod;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@JniClass
public class Posix extends CLibrary {

    /**
     * <code><pre>
     * int posix_memalign(void **ptrRef, size_t alignment, size_t len);
     * </pre></code>
     */
    @JniMethod(conditional="defined(HAVE_POSIX_MEMALIGN_FUNCTION)")
    public static final native int posix_memalign(
            @JniArg(cast="void **") long ptrRef[], 
            @JniArg(cast="size_t")  long alignment, 
            @JniArg(cast="size_t")  long len);
    
}
