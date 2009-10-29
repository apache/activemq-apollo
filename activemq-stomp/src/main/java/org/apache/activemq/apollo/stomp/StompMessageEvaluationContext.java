package org.apache.activemq.apollo.stomp;

import org.apache.activemq.filter.Expression;
import org.apache.activemq.filter.FilterException;
import org.apache.activemq.filter.MessageEvaluationContext;

public class StompMessageEvaluationContext implements MessageEvaluationContext {

    public <T> T getBodyAs(Class<T> type) throws FilterException {
        return null;
    }

    public <T> T getDestination() {
        return null;
    }

    public Object getLocalConnectionId() {
        // TODO Auto-generated method stub
        return null;
    }

    public Expression getPropertyExpression(String name) {
        // TODO Auto-generated method stub
        return null;
    }

    public void setDestination(Object destination) {
        // TODO Auto-generated method stub

    }

}
