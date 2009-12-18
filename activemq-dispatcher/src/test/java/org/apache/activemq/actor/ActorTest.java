/**************************************************************************************
 * Copyright (C) 2009 Progress Software, Inc. All rights reserved.                    *
 * http://fusesource.com                                                              *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the AGPL license      *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package org.apache.activemq.actor;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.apache.activemq.dispatch.DispatchQueue;
import org.apache.activemq.dispatch.Dispatcher;
import org.apache.activemq.dispatch.DispatcherConfig;
import org.apache.activemq.dispatch.internal.advanced.AdvancedDispatcher;

/**
 * ActorTest
 * <p>
 * Description:
 * </p>
 * 
 * @author cmacnaug
 * @version 1.0
 */
public class ActorTest extends TestCase {

    
    interface TestObjectActor {
        public void actorInvocation(CountDownLatch latch);
    }
    
    public static class TestObject implements TestObjectActor {
        public void actorInvocation(CountDownLatch latch) {
            latch.countDown();
        }
    }

    public void testActorInvocation() throws Exception {
        Dispatcher advancedSystem = new AdvancedDispatcher(new DispatcherConfig());
        advancedSystem.resume();

        DispatchQueue queue = advancedSystem.createSerialQueue("test");
        TestObjectActor actor = ActorProxy.create(TestObjectActor.class, new TestObject(), queue);

        CountDownLatch latch = new CountDownLatch(1);
        actor.actorInvocation(latch);
        assertTrue(latch.await(1, TimeUnit.SECONDS));

        queue.suspend();
        latch = new CountDownLatch(1);
        actor.actorInvocation(latch);
        assertFalse("Suspended Queue shouldn't invoked method", latch.await(2, TimeUnit.SECONDS));

        queue.resume();
        assertTrue("Resumed Queue should invoke method", latch.await(2, TimeUnit.SECONDS));
    }
}
