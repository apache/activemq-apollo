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

import java.util.concurrent.atomic.AtomicReference;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public abstract class Actor {
    
    public static enum ActorState {
        STOPPED,
        STARTING,
        RUNNING,
        STOPPING,
    }

    protected String name;
	protected Thread thread;
	protected AtomicReference<Actor.ActorState> state = new AtomicReference<Actor.ActorState>(ActorState.STOPPED);
	
    public Actor() {
    }
    
	public Actor(String name) {
		this.name = name;
	}

	public void start() {
		if( state.compareAndSet(ActorState.STOPPED, ActorState.STARTING) ) {
			thread = new Thread(new Runnable() {
				public void run() {
					if( state.compareAndSet(ActorState.STARTING, ActorState.RUNNING) ) {
						try {
							while( state.get()==ActorState.RUNNING ) {
							    Actor.this.run();
							}
						} catch (Exception e) {
							e.printStackTrace();
						} finally {
							state.set(ActorState.STOPPED);
						}
					}
				}
			}, name);
			thread.start();
		}
	}
	
	public void stop() {
		state.compareAndSet(ActorState.RUNNING, ActorState.STOPPING);
	}
	
	public void waitForStop() throws InterruptedException {
		stop();
		thread.join();
	}
	
	public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public AtomicReference<Actor.ActorState> getState() {
        return state;
    }

    abstract protected void run() throws Exception;
}