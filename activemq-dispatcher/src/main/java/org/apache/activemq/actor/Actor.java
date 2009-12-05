/**************************************************************************************
 * Copyright (C) 2009 Progress Software, Inc. All rights reserved.                    *
 * http://fusesource.com                                                              *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the AGPL license      *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package org.apache.activemq.actor;

import java.io.FileOutputStream;
import java.lang.reflect.Method;

import net.sf.cglib.core.DefaultGeneratorStrategy;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

import org.apache.activemq.dispatch.DispatchQueue;

/**
 * Allows creation of wrapper objects that intercept {@link Message} annotated
 * methods and dispatch them asynchronously on a given {@link DispatchQueue}.
 * 
 * @author cmacnaug
 * @version 1.0
 */
public class Actor {

    public static <T> T create(T target, DispatchQueue queue, Class<?>... interfaces) {

        validateMessageMethods(target);
        Enhancer e = new Enhancer();
        e.setSuperclass(target.getClass());
        e.setInterfaces(interfaces);
        e.setCallback(new ActorMethodInterceptor(queue));
//      Un-comment the following if you want store the generated class file:
//        e.setStrategy(new DefaultGeneratorStrategy() {
//            protected byte[] transform(byte[] b) {
//                try {
//                    FileOutputStream os = new FileOutputStream("test.class");
//                    os.write(b);
//                    os.close();
//                } catch (Throwable e) {
//                    e.printStackTrace();
//                }
//                return b;
//            }
//        });

        return (T) e.create();
    }

    private static final <T> void validateMessageMethods(T target) throws IllegalArgumentException {
        for (Method m : target.getClass().getDeclaredMethods()) {
            if (isMessage(m)) {
                if (m.getReturnType() == null) {
                    throw new IllegalArgumentException("Illegal method declaration: " + m + ". Actor methods must return void");
                }
                if (m.getExceptionTypes().length > 0) {
                    throw new IllegalArgumentException("Illegal method declaration: " + m + ". Actor methods must not throw exceptions");
                }
            }
        }
    }

    /**
     * Tests if a given method has a Message annotation
     * 
     * @param method
     *            The mehod.
     */
    private static boolean isMessage(Method method) {
        if (method.isAnnotationPresent(Message.class)) {
            return true;
        }
        if (method.getDeclaringClass().isAnnotationPresent(Message.class)) {
            return true;
        }
        return false;
    }

    private static class ActorMethodInterceptor implements MethodInterceptor {

        private final DispatchQueue queue;

        ActorMethodInterceptor(DispatchQueue queue) {
            this.queue = queue;
        }

        /*
         * (non-Javadoc)
         * 
         * @see net.sf.cglib.proxy.MethodInterceptor#intercept(java.lang.Object,
         * java.lang.reflect.Method, java.lang.Object[],
         * net.sf.cglib.proxy.MethodProxy)
         */
        public Object intercept(final Object obj, Method method, final Object[] args, final MethodProxy proxy) throws Throwable {
            if (isMessage(method)) {
                queue.dispatchAsync(new Runnable() {
                    public void run() {
                        try {
                            proxy.invokeSuper(obj, args);
                        } catch (Throwable thrown) {
                            throw new IllegalStateException(thrown);
                        }
                    }
                });
                return null;
            } else {
                return proxy.invokeSuper(obj, args);
            }
        }
    }

}
