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
package org.apache.hawtdb.internal.index;

import org.apache.activemq.util.marshaller.LongMarshaller;
import org.apache.activemq.util.marshaller.StringMarshaller;
import org.apache.hawtdb.api.BTreeIndexFactory;
import org.apache.hawtdb.api.Index;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class DefferedBTreeIndexTest extends BTreeIndexTest {

    @Override
    protected Index<String, Long> createIndex(int page) {
        BTreeIndexFactory<String,Long> factory = new BTreeIndexFactory<String,Long>();
        factory.setKeyMarshaller(StringMarshaller.INSTANCE);
        factory.setValueMarshaller(LongMarshaller.INSTANCE);
        factory.setDeferredEncoding(true);
        if( page==-1 ) {
            return factory.create(tx, tx.allocator().alloc(1));
        } else {
            return factory.open(tx, page);
        }
    }
    
}
