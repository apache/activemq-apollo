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
package org.apache.activemq.apollo;

import junit.framework.Test;
import org.apache.activemq.apollo.test.JmsResourceProvider;


/**
 * 
 */
public class JmsTopicTransactionTest extends JmsTransactionTestSupport {

    public static Test suite() {
        return suite(JmsTopicTransactionTest.class);
    }

    /**
     * @see org.apache.activemq.apollo.JmsTransactionTestSupport#getJmsResourceProvider()
     */
    protected JmsResourceProvider getJmsResourceProvider() {
        JmsResourceProvider p = new JmsResourceProvider(this);
        p.setTopic(true);
        p.setDurableName("testsub");
        p.setClientID("testclient");
        return p;
    }

    @Override
    public void runBare() throws Throwable {
        super.runBare();    //To change body of overridden methods use File | Settings | File Templates.
    }


}
