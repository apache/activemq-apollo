package org.apache.activemq.actor;

import org.apache.activemq.dispatch.internal.AbstractSerialDispatchQueue;
import org.junit.Test;

import static java.lang.String.*;

public class ActorBenchmark {

    
    public static class PizzaService implements IPizzaService
    {
        long counter;
        
        @Message
        public void order(long count)
        {
            counter += count;
        }
    }
    
//    @Test
    public void benchmarkCGLibProxy() throws Exception {
        String name = "cglib proxy";
        PizzaService service = new PizzaService();
        IPizzaService proxy = Actor.create(service, createQueue());
        benchmark(name, service, proxy);
    }

    @Test
    public void benchmarkCustomProxy() throws Exception {
        String name = "custom proxy";
        PizzaService service = new PizzaService();
        IPizzaService proxy = new PizzaServiceCustomProxy(service, createQueue());
        benchmark(name, service, proxy);
    }
    
    @Test
    public void benchmarkAsmProxy() throws Exception {
        String name = "asm proxy";
        PizzaService service = new PizzaService();
        IPizzaService proxy = AsmActor.create(IPizzaService.class, service, createQueue());
        benchmark(name, service, proxy);
    }

    private AbstractSerialDispatchQueue createQueue() {
        return new AbstractSerialDispatchQueue("mock queue") {
            public void dispatchAsync(Runnable runnable) {
                runnable.run();
            }
        };
    }

    private void benchmark(String name, PizzaService service, IPizzaService proxy) throws Exception {
        // warm it up..
        benchmark(proxy, 1000*1000);
        if( service.counter == 0)
            throw new Exception();
        
        int iterations = 1000*1000*100;
        
        long start = System.nanoTime();
        benchmark(proxy, iterations);
        long end = System.nanoTime();
        
        if( service.counter == 0)
            throw new Exception();

        double durationMS = 1.0d*(end-start)/1000000d;
        double rate = 1000d * iterations / durationMS;
        System.out.println(format("name: %s, duration: %,.3f ms, rate: %,.2f executions/sec", name, durationMS, rate));
    }


    private void benchmark(IPizzaService proxy, int iterations) {
        for( int i=0; i < iterations; i++ ) {
            proxy.order(1);
        }
    }
    
}
