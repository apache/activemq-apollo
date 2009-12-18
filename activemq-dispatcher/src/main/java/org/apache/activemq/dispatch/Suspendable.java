package org.apache.activemq.dispatch;

public interface Suspendable extends Retainable {
    public void suspend();
    public void resume();
}
