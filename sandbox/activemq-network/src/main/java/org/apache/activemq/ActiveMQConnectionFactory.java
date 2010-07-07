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
package org.apache.activemq;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;

public class ActiveMQConnectionFactory implements QueueConnectionFactory, TopicConnectionFactory {

    public ActiveMQConnectionFactory(Object vmConnectorURI) {
        // TODO Auto-generated constructor stub
    }

    public Connection createConnection() throws JMSException {
        // TODO Auto-generated method stub
        return null;
    }

    public Connection createConnection(String userName, String password) throws JMSException {
        // TODO Auto-generated method stub
        return null;
    }

    public QueueConnection createQueueConnection() throws JMSException {
        // TODO Auto-generated method stub
        return null;
    }

    public QueueConnection createQueueConnection(String userName, String password) throws JMSException {
        // TODO Auto-generated method stub
        return null;
    }

    public TopicConnection createTopicConnection() throws JMSException {
        // TODO Auto-generated method stub
        return null;
    }

    public TopicConnection createTopicConnection(String userName, String password) throws JMSException {
        // TODO Auto-generated method stub
        return null;
    }


}
