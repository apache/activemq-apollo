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
package org.apache.activemq.queue.actor.perf;

import java.util.ArrayList;

import org.apache.activemq.dispatch.Dispatcher;
import org.apache.activemq.dispatch.DispatcherConfig;
import org.apache.activemq.flow.Commands.Destination;
import org.apache.activemq.flow.Commands.Destination.DestinationBean;
import org.apache.activemq.flow.Commands.Destination.DestinationBuffer;
import org.apache.activemq.util.Mapper;
import org.fusesource.hawtbuf.AsciiBuffer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class MockBrokerTest {

    protected static final int PERFORMANCE_SAMPLES = 3;
    protected static final int SAMPLING_FREQUENCY = 5;

    protected static final int FANIN_COUNT = 10;
    protected static final int FANOUT_COUNT = 10;

    protected static final int PRIORITY_LEVELS = 10;
    protected static final boolean USE_INPUT_QUEUES = false;

    // Set to put senders and consumers on separate brokers.
    protected boolean multibroker = false;

    // Set to mockup up ptp:
    protected boolean ptp = false;

    // Set to use tcp IO
    protected boolean tcp = false;
    // set to force marshalling even in the NON tcp case.
    protected boolean forceMarshalling = false;

    protected String sendBrokerURI;
    protected String receiveBrokerURI;

    // Set's the number of threads to use:
    static protected final int threadsPerDispatcher = Runtime.getRuntime().availableProcessors();
    protected boolean usePartitionedQueue = false;

    protected ArrayList<MockBroker> brokers = new ArrayList<MockBroker>();
    protected MockBroker sendBroker;
    protected MockBroker rcvBroker;
    protected MockClient client;

    protected static Dispatcher dispatcher;

    static public final Mapper<Long, Message> KEY_MAPPER = new Mapper<Long, Message>() {
        public Long map(Message element) {
            return element.getMsgId();
        }
    };
    static public final Mapper<Integer, Message> PARTITION_MAPPER = new Mapper<Integer, Message>() {
        public Integer map(Message element) {
            // we modulo 10 to have at most 10 partitions which the producers
            // gets split across.
            return (int) (element.getProducerId() % 10);
        }
    };

    @BeforeClass
    static public void setUpSuite() throws Exception {
        dispatcher = DispatcherConfig.create("test", threadsPerDispatcher);
        dispatcher.resume();
    }
    
    @AfterClass
    static public void tearDownSuite() throws Exception {
        if( dispatcher!=null ) {
            dispatcher.release();
        }
    }
    
    
    @Before
    public void setUp() throws Exception {
        if (tcp) {
            sendBrokerURI = "tcp://localhost:10000?wireFormat=proto";
            receiveBrokerURI = "tcp://localhost:20000?wireFormat=proto";
        } else {
            if (forceMarshalling) {
                sendBrokerURI = "pipe://SendBroker?wireFormat=proto&marshal=true";
                receiveBrokerURI = "pipe://ReceiveBroker?wireFormat=proto&marshal=true";
            } else {
                sendBrokerURI = "pipe://SendBroker?wireFormat=proto";
                receiveBrokerURI = "pipe://ReceiveBroker?wireFormat=proto";
            }
        }
    }

    @Test
    public void test_1_1_0() throws Exception {

        client = new MockClient();
        client.setNumProducers(1);
        client.setDestCount(1);
        client.setNumConsumers(0);

        createConnections("test_1_1_0", 1);
        runTestCase();
    }

    @Test
    public void test_1_1_1() throws Exception {
        client = new MockClient();
        client.setNumProducers(1);
        client.setDestCount(1);
        client.setNumConsumers(1);

        createConnections("test_1_1_1", 1);
        runTestCase();
    }

    @Test
    public void test_10_10_10() throws Exception {
        client = new MockClient();
        client.setNumProducers(FANIN_COUNT);
        client.setDestCount(FANIN_COUNT);
        client.setNumConsumers(FANOUT_COUNT);

        createConnections("test_10_10_10", FANIN_COUNT);
        runTestCase();
    }

    @Test
    public void test_10_1_10() throws Exception {
        client = new MockClient();
        client.setNumProducers(FANIN_COUNT);
        client.setDestCount(1);
        client.setNumConsumers(FANOUT_COUNT);

        createConnections("test_10_1_10", 1);
        runTestCase();
    }

    @Test
    public void test_10_1_1() throws Exception {
        client = new MockClient();
        client.setNumProducers(FANIN_COUNT);
        client.setDestCount(1);
        client.setNumConsumers(1);

        createConnections("test_10_1_1", 1);
//        setProducerThinkTime(1);
        runTestCase();
    }

    private void setProducerThinkTime(int thinkTime) {
        for( int i=0; i < client.getNumProducers(); i++ ) {
            client.producer(i).setThinkTime(thinkTime);
        }
    }

    @Test
    public void test_1_1_10() throws Exception {
        client = new MockClient();
        client.setNumProducers(1);
        client.setDestCount(1);
        client.setNumConsumers(FANOUT_COUNT);

        createConnections("test_1_1_10", 1);
        runTestCase();
    }

    @Test
    public void test_2_2_2() throws Exception {
        client = new MockClient();
        client.setNumProducers(2);
        client.setDestCount(2);
        client.setNumConsumers(2);

        createConnections("test_2_2_2", 2);
        runTestCase();
    }

    /**
     * Tests 2 producers sending to 1 destination with 2 consumres, but with
     * consumers set to select only messages from each producer. 1 consumers is
     * set to slow, the other producer should be able to send quickly.
     * 
     * @throws Exception
     */
    @Test
    public void test_2_2_2_SlowConsumer() throws Exception {
        client = new MockClient();
        client.setNumProducers(2);
        client.setDestCount(2);
        client.setNumConsumers(2);

        createConnections("test_2_2_2_SlowConsumer", 2);
        client.consumer(0).setThinkTime(50);
        runTestCase();

    }

    @Test
    public void test_2_2_2_Selector() throws Exception {
        client = new MockClient();
        client.setNumProducers(2);
        client.setDestCount(2);
        client.setNumConsumers(2);

        createConnections("test_2_2_2_Selector", 2);

        // Add properties to match producers to their consumers
        for (int i = 0; i < 2; i++) {
            String property = "match" + i;
            client.consumer(i).setSelector(property);
            client.producer(i).setProperty(property);
        }

        runTestCase();
    }

    /**
     * Test sending with 1 high priority sender. The high priority sender should
     * have higher throughput than the other low priority senders.
     * 
     * @throws Exception
     */
    @Test
    public void test_2_1_1_HighPriorityProducer() throws Exception {

        client = new MockClient();
        client.setNumProducers(2);
        client.setNumConsumers(1);
        client.setDestCount(1);

        createConnections("test_2_1_1_HighPriorityProducer", 1);
        ProducerConnection producer = client.producer(0);
        client.includeInRateReport(producer);
        producer.setPriority(1);
        producer.getRate().setName("High Priority Producer Rate");

        client.consumer(0).setThinkTime(1);

        runTestCase();
    }

    /**
     * Test sending with 1 high priority sender. The high priority sender should
     * have higher throughput than the other low priority senders.
     * 
     * @throws Exception
     */
    @Test
    public void test_2_1_1_MixedHighPriorityProducer() throws Exception {
        client = new MockClient();
        client.setNumProducers(2);
        client.setNumConsumers(1);
        client.setDestCount(1);

        createConnections("test_2_1_1_MixedHighPriorityProducer", 1);
        ProducerConnection producer = client.producer(0);
        producer.setPriority(1);
        producer.setPriorityMod(3);
        producer.getRate().setName("High Priority Producer Rate");

        client.consumer(0).setThinkTime(1);
        runTestCase();
    }

    private void createConnections(String testName, int destCount) throws Exception {

        if (multibroker) {
            sendBroker = createBroker("SendBroker", sendBrokerURI);
            rcvBroker = createBroker("RcvBroker", receiveBrokerURI);
            brokers.add(sendBroker);
            brokers.add(rcvBroker);
        } else {
            sendBroker = rcvBroker = createBroker("Broker", sendBrokerURI);
            brokers.add(sendBroker);
        }

        DestinationBuffer[] dests = new DestinationBuffer[destCount];

        for (int i = 0; i < destCount; i++) {
            DestinationBean bean = new DestinationBean();
            bean.setName(new AsciiBuffer("dest" + (i + 1)));
            bean.setPtp(ptp);
            dests[i] = bean.freeze();
            if (ptp) {
                MockQueue queue = createQueue(sendBroker, dests[i]);
                sendBroker.addQueue(queue);
                if (multibroker) {
                    queue = createQueue(rcvBroker, dests[i]);
                    rcvBroker.addQueue(queue);
                }
            }
        }

        // Configure Client:
        client.setDispatcher(dispatcher);
        client.setNumPriorities(PRIORITY_LEVELS);
        client.setSendBrokerURI(sendBroker.getUri());
        client.setReceiveBrokerURI(rcvBroker.getUri());
        client.setPerformanceSamples(PERFORMANCE_SAMPLES);
        client.setSamplingFrequency(1000 * SAMPLING_FREQUENCY);
        client.setThreadsPerDispatcher(threadsPerDispatcher);
        client.setPtp(ptp);
        client.setTestName(testName);
        client.createConnections();
    }

    private MockQueue createQueue(MockBroker broker, Destination destination) {
        MockQueue queue = new MockQueue();
        queue.setBroker(broker);
        queue.setDestination(destination);
        queue.setKeyExtractor(KEY_MAPPER);
        if (usePartitionedQueue) {
            queue.setPartitionMapper(PARTITION_MAPPER);
        }
        return queue;
    }

    private MockBroker createBroker(String name, String uri) {
        MockBroker broker = new MockBroker();
        broker.setName(name);
        broker.setUri(uri);
        broker.setDispatcher(dispatcher);
        broker.setUseInputQueues(USE_INPUT_QUEUES);
        return broker;
    }

    private void runTestCase() throws Exception {
        // Start 'em up.
        startServices();
        try {
            client.runTest();
        } finally {
            System.out.println("Shutting down..");
            stopServices();
        }
    }

    private void stopServices() throws Exception {
        for (MockBroker broker : brokers) {
            broker.stopServices();
        }
    }

    private void startServices() throws Exception {

        for (MockBroker broker : brokers) {
            broker.startServices();
        }
    }

}
