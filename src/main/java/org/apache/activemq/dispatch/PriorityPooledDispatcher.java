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
package org.apache.activemq.dispatch;

import java.util.ArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.dispatch.ExecutionLoadBalancer.ExecutionTracker;

public class PriorityPooledDispatcher implements IDispatcher, PooledDispatcher {
	private final String name;

	private static final ThreadLocal<PooledDispatchContext> dispatchContext = new ThreadLocal<PooledDispatchContext>();
	private static final ThreadLocal<PoolableDispatcher> dispatcher = new ThreadLocal<PoolableDispatcher>();

	private final ArrayList<PriorityDispatcher> dispatchers = new ArrayList<PriorityDispatcher>();

	final AtomicBoolean started = new AtomicBoolean();
	final AtomicBoolean shutdown = new AtomicBoolean();

	private int roundRobinCounter = 0;
	private final int size;
	private final boolean DEBUG = false;

	private final ExecutionLoadBalancer loadBalancer;

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.activemq.dispatch.IDispatcher#createPriorityExecutor(int)
	 */
	public Executor createPriorityExecutor(final int priority) {
		return new Executor() {
			public void execute(final Runnable runnable) {
				chooseDispatcher().dispatch(new RunnableAdapter(runnable), priority);
			}
		};
	}

	public PriorityPooledDispatcher(String name, int size, int priorities) {
		this.name = name;
		this.size = size;
		loadBalancer = new SimpleLoadBalancer();
		// Create all the workers.
		for (int i = 0; i < size; i++) {
			PriorityDispatcher dispatcher = new PriorityDispatcher(name + "-" + (i + 1), priorities, this);
			dispatchers.add(dispatcher);
		}
	}

	public DispatchContext register(Dispatchable dispatchable, String name) {
		return createPooledDispatchContext(chooseDispatcher().createPoolableDispatchContext(dispatchable, name));
	}

	/**
	 * @see org.apache.activemq.dispatch.IDispatcher#start()
	 */
	public synchronized final void start() {
		loadBalancer.start();
		if (started.compareAndSet(false, true)) {
			// Create all the workers.
			for (int i = 0; i < size; i++) {
				dispatchers.get(i).start();
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.activemq.dispatch.IDispatcher#shutdown()
	 */
	public synchronized final void shutdown() throws InterruptedException {
		shutdown.set(true);
		for (PriorityDispatcher dispatcher : dispatchers) {
			dispatcher.shutdown();
		}
		loadBalancer.stop();
	}

	private PriorityDispatcher chooseDispatcher() {
		PriorityDispatcher d = PriorityDispatcher.dispatcher.get();
		if (d == null) {
			synchronized (dispatchers) {
				if (++roundRobinCounter >= size) {
					roundRobinCounter = 0;
				}
				return dispatchers.get(roundRobinCounter);
			}
		} else {
			return d;
		}
	}

	public void execute(final Runnable runnable) {
		chooseDispatcher().dispatch(new RunnableAdapter(runnable), 0);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.activemq.dispatch.IDispatcher#schedule(java.lang.Runnable,
	 *      long, java.util.concurrent.TimeUnit)
	 */
	public void schedule(final Runnable runnable, long delay, TimeUnit timeUnit) {
		chooseDispatcher().schedule(runnable, delay, timeUnit);
	}

	public PooledDispatchContext createPooledDispatchContext(PoolableDispatchContext context) {
		return new PriorityPooledDispatchContext(context);
	}

	/**
	 * A Dispatcher must call this to indicate that is has started it's dispatch
	 * loop.
	 */
	public void onDispatcherStarted(PoolableDispatcher d) {
		dispatcher.set(d);
		loadBalancer.addDispatcher(d);
	}

	/**
	 * A Dispatcher must call this when exiting it's dispatch loop
	 */
	public void onDispatcherStopped(PoolableDispatcher d) {
		loadBalancer.removeDispatcher(d);
	}

	/**
	 * ExecutionGraphNode tracks dispatch information for a
	 * MappableDispatchContext.
	 * 
	 */
	public class PriorityPooledDispatchContext implements PooledDispatchContext {
		private final ExecutionTracker tracker;

		private PoolableDispatchContext context;
		private PoolableDispatcher currentOwner;
		private int priority;
		private boolean dispatchRequested = false;
		private PoolableDispatcher updateDispatcher = null;
		private boolean closed = false;

		PriorityPooledDispatchContext(PoolableDispatchContext context) {
			this.context = context;
			this.context.setPooledDispatchContext(this);
			this.currentOwner = context.getDispatcher();
			this.tracker = loadBalancer.createExecutionTracker(this);

		}

		public final void startingDispatch() {
			dispatchContext.set(this);
		}

		public final void finishedDispatch() {
			dispatchContext.set(null);
		}

		public final void assignToNewDispatcher(PoolableDispatcher newDispatcher) {
			synchronized (this) {

				// If we're already set to this dispatcher
				if (newDispatcher == currentOwner) {
					if (updateDispatcher == null || updateDispatcher == newDispatcher) {
						return;
					}
				}

				updateDispatcher = newDispatcher;
				if (DEBUG)
					System.out.println(getName() + " updating to " + context.getDispatcher());
			}
			context.onForeignThreadUpdate();
		}

		public void requestDispatch() {

			PoolableDispatcher callingDispatcher = dispatcher.get();

			tracker.onDispatchRequest(callingDispatcher, dispatchContext.get());

			// Otherwise this is coming off another thread, so we need to
			// synchronize
			// to protect against ownership changes:
			synchronized (this) {
				// If the owner of this context is the calling thread, then
				// delegate to the dispatcher.
				if (currentOwner == callingDispatcher) {

					context.requestDispatch();
					return;
				}

				dispatchRequested = true;
			}
			context.onForeignThreadUpdate();
		}

		public void updatePriority(int priority) {
			if (this.priority == priority) {
				return;
			}
			// Otherwise this is coming off another thread, so we need to
			// synchronize to protect against ownership changes:
			synchronized (this) {
				this.priority = priority;

				IDispatcher callingDispatcher = dispatcher.get();

				// If the owner of this context is the calling thread, then
				// delegate to the dispatcher.
				if (currentOwner == callingDispatcher) {

					context.updatePriority(priority);
					return;
				}
			}
			context.onForeignThreadUpdate();
		}

		public void processForeignUpdates() {
			boolean ownerChange = false;
			synchronized (this) {

				if (closed) {
					context.close();
					return;
				}

				if (updateDispatcher != null) {
					// Close the old context:
					if (DEBUG) {
						System.out.println("Assigning " + getName() + " to " + updateDispatcher);
					}
					context.close();

					currentOwner = updateDispatcher;
					updateDispatcher = null;
					context = currentOwner.createPoolableDispatchContext(context.getDispatchable(), context.getName());
					dispatchRequested = true;
					context.updatePriority(priority);
					context.setPooledDispatchContext(this);
					ownerChange = true;
				} else {
					context.updatePriority(priority);

					if (dispatchRequested) {
						context.requestDispatch();
						dispatchRequested = false;
					}
				}
			}

			if (ownerChange) {
				context.onForeignThreadUpdate();
			}
		}

		public void close() {
			tracker.close();
			synchronized (this) {
				IDispatcher callingDispatcher = dispatcher.get();

				// If the owner of this context is the calling thread, then
				// delegate to the dispatcher.
				if (currentOwner == callingDispatcher) {
					context.close();
					return;
				}
			}
			context.onForeignThreadUpdate();
		}

		public final String toString() {
			return context.toString();
		}

		public Dispatchable getDispatchable() {
			return context.getDispatchable();
		}

		public String getName() {
			return context.getName();
		}
	}

	public String toString() {
		return name;
	}
}
