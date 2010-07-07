package org.apache.activemq.broker.openwire;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.jms.DeliveryMode;
import javax.jms.JMSException;

import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.apollo.filter.Expression;
import org.apache.activemq.filter.FilterException;
import org.apache.activemq.filter.MessageEvaluationContext;
import org.fusesource.hawtbuf.Buffer;

public class OpenwireMessageEvaluationContext implements MessageEvaluationContext {

    private Message message;

    public OpenwireMessageEvaluationContext() {
    }
    public OpenwireMessageEvaluationContext(Message message) {
        this.message = message;
    }

    public Message getMessage() {
        return message;
    }

    public void setMessage(Message message) {
        this.message = message;
    }

    private static final Map<String, Expression> JMS_PROPERTY_EXPRESSIONS = new HashMap<String, Expression>();
    private Object destination;

    static {
        JMS_PROPERTY_EXPRESSIONS.put("JMSDestination", new Expression() {
            public Object evaluate(MessageEvaluationContext mc) {
                Message message = ((OpenwireMessageEvaluationContext) mc).message;
                ActiveMQDestination dest = message.getOriginalDestination();
                if (dest == null) {
                    dest = message.getDestination();
                }
                if (dest == null) {
                    return null;
                }
                return dest.toString();
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSReplyTo", new Expression() {
            public Object evaluate(MessageEvaluationContext mc) {
                Message message = ((OpenwireMessageEvaluationContext) mc).message;
                if (message.getReplyTo() == null) {
                    return null;
                }
                return message.getReplyTo().toString();
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSType", new Expression() {
            public Object evaluate(MessageEvaluationContext mc) {
                Message message = ((OpenwireMessageEvaluationContext) mc).message;
                return message.getType();
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSDeliveryMode", new Expression() {
            public Object evaluate(MessageEvaluationContext mc) {
                Message message = ((OpenwireMessageEvaluationContext) mc).message;
                return Integer.valueOf(message.isPersistent() ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSPriority", new Expression() {
            public Object evaluate(MessageEvaluationContext mc) {
                Message message = ((OpenwireMessageEvaluationContext) mc).message;
                return Integer.valueOf(message.getPriority());
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSMessageID", new Expression() {
            public Object evaluate(MessageEvaluationContext mc) {
                Message message = ((OpenwireMessageEvaluationContext) mc).message;
                if (message.getMessageId() == null) {
                    return null;
                }
                return message.getMessageId().toString();
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSTimestamp", new Expression() {
            public Object evaluate(MessageEvaluationContext mc) {
                Message message = ((OpenwireMessageEvaluationContext) mc).message;
                return Long.valueOf(message.getTimestamp());
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSCorrelationID", new Expression() {
            public Object evaluate(MessageEvaluationContext mc) {
                Message message = ((OpenwireMessageEvaluationContext) mc).message;
                return message.getCorrelationId();
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSExpiration", new Expression() {
            public Object evaluate(MessageEvaluationContext mc) {
                Message message = ((OpenwireMessageEvaluationContext) mc).message;
                return Long.valueOf(message.getExpiration());
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSRedelivered", new Expression() {
            public Object evaluate(MessageEvaluationContext mc) {
                Message message = ((OpenwireMessageEvaluationContext) mc).message;
                return Boolean.valueOf(message.isRedelivered());
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSXDeliveryCount", new Expression() {
            public Object evaluate(MessageEvaluationContext mc) {
                Message message = ((OpenwireMessageEvaluationContext) mc).message;
                return Integer.valueOf(message.getRedeliveryCounter() + 1);
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSXGroupID", new Expression() {
            public Object evaluate(MessageEvaluationContext mc) {
                Message message = ((OpenwireMessageEvaluationContext) mc).message;
                return message.getGroupID();
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSXGroupSeq", new Expression() {
            public Object evaluate(MessageEvaluationContext mc) {
                Message message = ((OpenwireMessageEvaluationContext) mc).message;
                return new Integer(message.getGroupSequence());
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSXProducerTXID", new Expression() {
            public Object evaluate(MessageEvaluationContext mc) {
                Message message = ((OpenwireMessageEvaluationContext) mc).message;
                TransactionId txId = message.getOriginalTransactionId();
                if (txId == null) {
                    txId = message.getTransactionId();
                }
                if (txId == null) {
                    return null;
                }
                return new Integer(txId.toString());
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSActiveMQBrokerInTime", new Expression() {
            public Object evaluate(MessageEvaluationContext mc) {
                Message message = ((OpenwireMessageEvaluationContext) mc).message;
                return Long.valueOf(message.getBrokerInTime());
            }
        });
        JMS_PROPERTY_EXPRESSIONS.put("JMSActiveMQBrokerOutTime", new Expression() {
            public Object evaluate(MessageEvaluationContext mc) {
                Message message = ((OpenwireMessageEvaluationContext) mc).message;
                return Long.valueOf(message.getBrokerOutTime());
            }
        });
    }

    public Expression getPropertyExpression(final String name) {
        Expression expression = JMS_PROPERTY_EXPRESSIONS.get(name);
        if( expression == null ) {
            expression = new Expression() {
                public Object evaluate(MessageEvaluationContext mc) throws FilterException {
                    try {
                        Message message = ((OpenwireMessageEvaluationContext) mc).message;
                        return message.getProperty(name);
                    } catch (IOException e) {
                        throw new FilterException(e);
                    }
                }
            };
        }
        return expression;
    }

    public <T> T getBodyAs(Class<T> type) throws FilterException {
        try {
            if( type == String.class ) {
                if ( message instanceof ActiveMQTextMessage ) {
                    return type.cast(((ActiveMQTextMessage)message).getText());
                }
            }
            if( type == Buffer.class ) {
                if ( message instanceof ActiveMQBytesMessage ) {
                    ActiveMQBytesMessage bm = ((ActiveMQBytesMessage)message);
                    byte data[] = new byte[(int) bm.getBodyLength()];
                    bm.readBytes(data);
                    return type.cast(new Buffer(data));
                }
            }
            return null;
        } catch (JMSException e) {
            throw new FilterException(e);
        }
    }

    public <T> T getDestination() {
        return (T) destination;
    }
    public Object getLocalConnectionId() {
        throw new UnsupportedOperationException();
    }
    public void setDestination(Object destination) {
        this.destination = destination;
    }

}
