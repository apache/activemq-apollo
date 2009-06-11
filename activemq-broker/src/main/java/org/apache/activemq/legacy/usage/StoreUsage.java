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
package org.apache.activemq.legacy.usage;


/**
 * @deprecated The entire 'org.apache.activemq.legacy' package will hopefully go away soon.
 */
@Deprecated
public class StoreUsage extends Usage<StoreUsage> {

    
    public StoreUsage() {
        super(null, null, 1.0f);
    }

    public StoreUsage(String name) {
        super(null, name, 1.0f);
    }

    public StoreUsage(StoreUsage parent, String name) {
        super(parent, name, 1.0f);
    }

    protected long retrieveUsage() {
        return 0;
    }

 
}
