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
package org.apache.hawtdb.internal;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class ActionActor<A extends Actor> extends Actor {
    
    private Action<A> action;
	
    public ActionActor() {
    }
    
	public ActionActor(String name, Action<A> action) {
		super(name);
        this.action = action;
        this.action.init(cast());
	}

    @SuppressWarnings("unchecked")
    private A cast() {
        return (A) this;
    }

	public void run() throws Exception {
		action.run(cast());
	}

    public Action<A> getAction() {
        return action;
    }

    public void setAction(Action<A> action) {
        this.action = action;
        this.action.init(cast());
    }
	
	
}