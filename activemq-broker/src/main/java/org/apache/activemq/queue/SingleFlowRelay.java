package org.apache.activemq.queue;

import org.apache.activemq.flow.Flow;
import org.apache.activemq.flow.FlowController;
import org.apache.activemq.flow.IFlowController;
import org.apache.activemq.flow.IFlowLimiter;
import org.apache.activemq.flow.ISourceController;
import org.apache.activemq.flow.ISinkController.FlowControllable;

public class SingleFlowRelay<E> extends AbstractFlowRelay<E> implements FlowControllable<E> {

    private final IFlowController<E> controller;

    public SingleFlowRelay(Flow flow, String name, IFlowLimiter<E> limiter) {
        super(name);
        FlowController<E> c = new FlowController<E>(this, flow, limiter, this);
        c.useOverFlowQueue(false);
        controller = c;
        super.onFlowOpened(controller);
    }

    public void add(E elem, ISourceController<?> source) {
        controller.add(elem, source);
    }

    public boolean offer(E elem, ISourceController<?> source) {
        return controller.offer(elem, source);
    }

    public void flowElemAccepted(ISourceController<E> controller, E elem) {
        drain.drain(elem, controller);
        if (autoRelease) {
            controller.elementDispatched(elem);
        }
    }

    @Override
    public String toString() {
        return getResourceName();
    }

}
