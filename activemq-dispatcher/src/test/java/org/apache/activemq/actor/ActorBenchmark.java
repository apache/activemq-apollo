package org.apache.activemq.actor;

import org.apache.activemq.dispatch.DispatchQueue;
import org.apache.activemq.dispatch.internal.AbstractSerialDispatchQueue;
import org.junit.Test;

import static java.lang.String.*;

public class ActorBenchmark {

    
    public static class PizzaService
    {
        long counter;
        
        @Message
        public long order(long count)
        {
            counter += count;
            return counter;
        }
    }
    
    
    @Test
    public void benchmark() throws Exception {
        
        String name = "default actor";
        PizzaService service = new PizzaService();
        
        // Directly execute the requests...
        DispatchQueue queue = new AbstractSerialDispatchQueue("") {
            public void dispatchAsync(Runnable runnable) {
                runnable.run();
            }
        };
        
        PizzaService proxy = Actor.create(service, queue);

        // warm it up..
        benchmark(proxy, 1000*1000);
        if( service.counter == 0)
            throw new Exception();
        
        int iterations = 1000*1000*1000;
        long start = System.nanoTime();
        benchmark(proxy, iterations);
        long end = System.nanoTime();
        
        if( service.counter == 0)
            throw new Exception();

        double durationMS = 1.0d*(end-start)/1000000d;
        double rate = 1000d * iterations / durationMS;
        System.out.println(format("name: %s, duration: %,.3f ms, rate: %,.2f executions/sec", name, durationMS, rate));
    }


    private void benchmark(PizzaService proxy, int iterations) {
        for( int i=0; i < iterations; i++ ) {
            proxy.order(1);
        }
    }
    
}
