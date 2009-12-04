/**************************************************************************************
 * Copyright (C) 2009 Progress Software, Inc. All rights reserved.                    *
 * http://fusesource.com                                                              *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the AGPL license      *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package org.apache.activemq.actor;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;


/**
 * Indicates that a given method, class or interface should be treated as an actor. 
 * 
 * @author cmacnaug 
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@java.lang.annotation.Target( { java.lang.annotation.ElementType.METHOD, java.lang.annotation.ElementType.TYPE })
public @interface Message {

}
