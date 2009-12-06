package org.apache.activemq.queue.perf;

import java.io.FileInputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.dispatch.Dispatcher;
import org.apache.activemq.dispatch.DispatcherConfig;
import org.apache.activemq.flow.Commands.Destination;
import org.apache.activemq.flow.Commands.Destination.DestinationBean;
import org.apache.activemq.flow.Commands.Destination.DestinationBuffer;
import org.apache.activemq.metric.MetricAggregator;
import org.apache.activemq.metric.MetricCounter;
import org.apache.activemq.metric.Period;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.buffer.AsciiBuffer;

public class MockClient {

    protected int performanceSamples = 3;
    protected int samplingFrequency = 5000;

    protected int numProducers = 1;
    protected int numConsumers = 1;
    protected int destCount = 1;
    protected int numPriorities = 1;
    protected boolean useInputQueues = false;

    // Set to mockup up ptp:
    protected boolean ptp = false;

    protected String sendBrokerURI;
    protected String receiveBrokerURI;

    // Sets the number of threads to use:
    protected int threadsPerDispatcher = Runtime.getRuntime().availableProcessors();

    protected MetricAggregator totalProducerRate = new MetricAggregator().name("Aggregate Producer Rate").unit("items");
    protected MetricAggregator totalConsumerRate = new MetricAggregator().name("Aggregate Consumer Rate").unit("items");
    protected ArrayList<MetricCounter> additionalReportMetrics = new ArrayList<MetricCounter>();
    protected boolean includeDetailedRates = false;

    protected Dispatcher dispatcher;

    public RemoteConsumer consumer(int index) {
        return consumers.get(index);
    }

    public RemoteProducer producer(int index) {
        return producers.get(index);
    }

    public int getThreadsPerDispatcher() {
        return threadsPerDispatcher;
    }

    public boolean isUseInputQueues() {
        return useInputQueues;
    }

    public void setUseInputQueues(boolean useInputQueues) {
        this.useInputQueues = useInputQueues;
    }
    
    public void setThreadsPerDispatcher(int threadPoolSize) {
        this.threadsPerDispatcher = threadPoolSize;
    }
    
    public void setIncludeDetailedRates(boolean includeDetailedRates) {
        this.includeDetailedRates = includeDetailedRates;
    }

    public boolean getIncludeDetailedRates() {
        return includeDetailedRates;
    }

    public void includeInRateReport(RemoteProducer producer) {
        additionalReportMetrics.add(producer.getRate());
    }

    public void includeInRateReport(RemoteConsumer consumer) {
        additionalReportMetrics.add(consumer.getRate());
    }
    
    public int getSamplingFrequency() {
        return samplingFrequency;
    }

    public void setSamplingFrequency(int samplingFrequency) {
        this.samplingFrequency = samplingFrequency;
    }


    public int getNumProducers() {
        return numProducers;
    }

    public void setNumProducers(int numProducers) {
        this.numProducers = numProducers;
    }

    public int getNumConsumers() {
        return numConsumers;
    }

    public void setNumConsumers(int numConsumers) {
        this.numConsumers = numConsumers;
    }

    public int getDestCount() {
        return destCount;
    }

    public void setDestCount(int destCount) {
        this.destCount = destCount;
    }

    public int getNumPriorities() {
        return numPriorities;
    }

    public void setNumPriorities(int numPriorities) {
        this.numPriorities = numPriorities;
    }

    public boolean isPtp() {
        return ptp;
    }

    public void setPtp(boolean ptp) {
        this.ptp = ptp;
    }

    public String getSendBrokerURI() {
        return sendBrokerURI;
    }

    public void setSendBrokerURI(String sendBrokerURI) {
        this.sendBrokerURI = sendBrokerURI;
    }

    public String getReceiveBrokerURI() {
        return receiveBrokerURI;
    }

    public void setReceiveBrokerURI(String receiveBrokerURI) {
        this.receiveBrokerURI = receiveBrokerURI;
    }

    public int getPerformanceSamples() {
        return performanceSamples;
    }

    
    protected final AtomicLong msgIdGenerator = new AtomicLong();

    final ArrayList<RemoteProducer> producers = new ArrayList<RemoteProducer>();
    final ArrayList<RemoteConsumer> consumers = new ArrayList<RemoteConsumer>();

    private String testName;

    private void createConsumer(int i, String connectUri, Destination destination) throws URISyntaxException {
        RemoteConsumer consumer = new RemoteConsumer();
        consumer.setDestination(destination);
        consumer.setName("consumer" + (i + 1));
        consumer.setTotalConsumerRate(totalConsumerRate);
        consumer.setDispatcher(dispatcher);
        consumer.setConnectUri(new URI(connectUri));
        consumer.setUseInputQueue(useInputQueues);
        consumers.add(consumer);
    }

    private void createProducer(int id, String connectUri, Destination destination) throws URISyntaxException {
        RemoteProducer producer = new RemoteProducer();
        producer.setProducerId(id + 1);
        producer.setName("producer" + (id + 1));
        producer.setDestination(destination);
        producer.setMessageIdGenerator(msgIdGenerator);
        producer.setTotalProducerRate(totalProducerRate);
        producer.setDispatcher(dispatcher);
        producer.setUseInputQueue(useInputQueues);
        producer.setConnectUri(new URI(connectUri));
        producers.add(producer);
    }

    private void reportRates() throws InterruptedException {
        System.out.println("Checking rates for test: " + getTestName() + ", " + (ptp ? "ptp" : "topic"));
        for (int i = 0; i < performanceSamples; i++) {
            Period p = new Period();
            Thread.sleep(samplingFrequency);
            System.out.println(totalProducerRate.getRateSummary(p));
            System.out.println(totalConsumerRate.getRateSummary(p));
            if (includeDetailedRates) {
                System.out.println(totalProducerRate.getChildRateSummary(p));
                System.out.println(totalConsumerRate.getChildRateSummary(p));
            }
            totalProducerRate.reset();
            totalConsumerRate.reset();
        }
    }

    public void setTestName(String testName) {
        this.testName = testName;
    }

    public void setPerformanceSamples(int samples) {
        this.performanceSamples = samples;
    }

    public String getTestName() {
        return testName;
    }

    public void setDispatcher(Dispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    public void runTest() throws Exception {
        getDispatcher().retain();

        // Start 'em up.
        startServices();
        try {
            reportRates();
        } finally {
            stopServices();
        }
    }

    private void startServices() throws Exception {
        AbstractTestConnection.setInShutdown(false, dispatcher);
        for (RemoteConsumer connection : consumers) {
            connection.start();
        }

        for (RemoteProducer connection : producers) {
            connection.start();
        }
    }

    private void stopServices() throws Exception {

        AbstractTestConnection.setInShutdown(true, dispatcher);
        for (RemoteProducer connection : producers) {
            connection.stop();
        }
        for (RemoteConsumer connection : consumers) {
            connection.stop();
        }
    }

    public void createConnections() throws Exception {

        DestinationBuffer[] dests = new DestinationBuffer[destCount];

        for (int i = 0; i < destCount; i++) {
            DestinationBean bean = new DestinationBean();
            bean.setName(new AsciiBuffer("dest" + (i + 1)));
            bean.setPtp(ptp);
            dests[i] = bean.freeze();
        }

        for (int i = 0; i < numProducers; i++) {
            Destination destination = dests[i % destCount];
            createProducer(i, sendBrokerURI, destination);
        }

        for (int i = 0; i < numConsumers; i++) {
            Destination destination = dests[i % destCount];
            createConsumer(i, receiveBrokerURI, destination);
        }
    }

    public Dispatcher getDispatcher() {
        return dispatcher;
    }

    protected Dispatcher createDispatcher() {
        if (dispatcher == null) {
            dispatcher = DispatcherConfig.create("client", threadsPerDispatcher);
        }
        return dispatcher;
    }

    /**
     * Run the broker as a standalone app
     * 
     * @param args
     *            The arguments.
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        MockClient test = new MockClient();
        test.createDispatcher();
        
        Properties props = new Properties();
        if (args.length > 0) {
            props.load(new FileInputStream(args[0]));
            IntrospectionSupport.setProperties(test, props);
        }
        System.out.println(IntrospectionSupport.toString(test));
        try
        {
            test.getDispatcher().retain();
            test.createConnections();
            test.runTest();
        }
        finally
        {
            test.getDispatcher().release();
        }
    }

}
