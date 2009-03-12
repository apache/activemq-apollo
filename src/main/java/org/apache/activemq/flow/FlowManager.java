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
package org.apache.activemq.flow;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Set;
 
public class FlowManager
{
    
    protected long resourceCounter = 0;
    
    private final HashMap <Long, IFlowResource> resources = new HashMap<Long, IFlowResource> ();
    private final HashMap <Long, Flow> flows = new HashMap<Long, Flow> ();
    private final HashMap <String, Flow> flowsByName = new HashMap<String, Flow> ();
    
    public synchronized void registerResource(IFlowResource resource)
    {
        resources.put(resource.getResourceId(), resource);
    }
    
    public synchronized Flow createFlow(String name, boolean dynamic)
    {
        //TODO Should assign the flow based off of the hashcode of the 
        //name, and handle collisions. 
        //TODO Also implement dynamic flows whereby resources participating
        //in a flow register with it, so that flows can be expired. 
        Flow flow = getFlow(name);
        if(flow != null)
        {
            return flow;
        }
        else
        {
            flow = new Flow(name, dynamic);
            flowsByName.put(name, flow);
            flows.put(flow.getFlowID(), flow);
            return flow;
        }
    }

    public synchronized IFlowResource getResource(long id) {
        return resources.get(id);
    }
    
    public synchronized Flow getFlow(long id) {
        return flows.get(id);
    }
    
    public synchronized Flow getFlow(String name) {
        return flowsByName.get(name);
    }
    
    public IFlowResource removeResource(long id) {
        return resources.remove(id);
    }
    
    public Collection<Flow> getRegisteredFlows() {
        return flows.values();
    }

    public Collection <IFlowResource> getRegisteredResources() {
        return resources.values();
    }

    /**
     * Returns a list of flow resources ids. Used for tooling.
     */
    public synchronized ArrayList<Long> getRegisteredResourceIDs()
    {
        Set<Long> rids = resources.keySet();
        ArrayList<Long> ret = new ArrayList<Long>(rids.size());
        ret.addAll(rids);
        return ret;
    }
    
    /**
     * Returns a list of flow resources ids. Used for tooling.
     */
    public synchronized ArrayList<Long> getRegisteredFlowIDs()
    {
        Set<Long> fids = flows.keySet();
        ArrayList<Long> ret = new ArrayList<Long>(fids.size());
        ret.addAll(fids);
        return ret;
    }
}
    /*
    private class FlowMetricsCollector
        implements Runnable
    {
        private final long m_collectionInterval = 10000;

        private Thread m_thread;

        private boolean m_started;

        FlowMetricsCollector()
        {

        }

        public void start()
        {
            synchronized ( this )
            {
                if ( m_started )
                {
                    return;
                }
                m_thread = new Thread( this, "FlowMetricsCollector" );
                m_started = true;
                m_thread.start();
            }
        }

        public void shutdown()
            throws InterruptedException
        {
            synchronized ( this )
            {
                if ( !m_started )
                {
                    return;
                }
                m_thread.interrupt();

                try
                {
                    m_thread.join();
                }
                finally
                {
                    m_thread = null;
                    m_started = false;
                }
            }
        }

        public void run()
        {
            while ( !Thread.currentThread().isInterrupted() )
            {
                long startTime = System.currentTimeMillis();

                Iterator metrics = getFlowMetrics().iterator();
                while ( metrics.hasNext() )
                {
                    FlowMetrics fm = (FlowMetrics) metrics.next();
                    fm.collect();
                }

                long endTime = System.currentTimeMillis();
                long timeToNext = m_collectionInterval - ( endTime - startTime );

                if ( timeToNext <= 0 )
                {
                    // TODO FLOWCONTROL should comment this out.
                    System.out.println( "Unable to maintain specified flow metrics collection interval of "
                        + m_collectionInterval + "ms, last collection took: " + ( endTime - startTime ) + "ms" );
                }
                else
                {
                    try
                    {
                        Thread.sleep( timeToNext );
                    }
                    catch ( InterruptedException ie )
                    {
                        return;
                    }
                }
            }
        }
    }*/
