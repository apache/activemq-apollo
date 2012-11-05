package org.apache.activemq.apollo.amqp.test

import org.apache.qpid.amqp_1_0.jms.impl.{ConnectionFactoryImpl, QueueImpl}
import javax.jms._

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */

class QpidJmsTest extends AmqpTestSupport {

  def createConnection: Connection = {
    val factory = new ConnectionFactoryImpl("localhost", port, "admin", "password")
    val connection = factory.createConnection
    connection.setExceptionListener(new ExceptionListener {
      def onException(exception: JMSException) {
        exception.printStackTrace
      }
    })
    connection.start
    return connection
  }


//  test("browse") {
//    val queue = new QueueImpl("queue://txqueue")
//    val connection = createConnection
//    val session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
//    val p = session.createProducer(queue)
//    val msg = session.createTextMessage("Hello World")
//    msg.setObjectProperty("x", 1)
//    p.send(msg)
//    val browser = session.createBrowser(queue)
//    val enumeration = browser.getEnumeration
//    while (enumeration.hasMoreElements) {
//      System.out.println("BROWSE " + enumeration.nextElement)
//    }
//    connection.close
//  }

  test("Send Nack Receive") {
    val queue = new QueueImpl("/queue/testqueue")
    val nMsgs = 1
    val dataFormat: String = "%01024d"

    var connection = createConnection
    var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val p = session.createProducer(queue)
    var i = 0
    while (i < nMsgs) {
      System.out.println("Sending " + i)
      p.send(session.createTextMessage(dataFormat.format(i)))
      i += 1
    }
    connection.close

    System.out.println("=======================================================================================")
    System.out.println(" failing a receive ")
    System.out.println("=======================================================================================")
    connection = createConnection
    session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE)
    var c = session.createConsumer(queue)
    i = 0
    while (i < 1) {
      val msg: TextMessage = c.receive.asInstanceOf[TextMessage]
      if (msg != null) {
        val s: String = msg.getText
        s should  be(dataFormat.format(i))
        System.out.println("Received: " + i)
        i += 1
      }
    }
    connection.close

    System.out.println("=======================================================================================")
    System.out.println(" receiving ")
    System.out.println("=======================================================================================")
    connection = createConnection
    session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    c = session.createConsumer(queue)
    i = 0
    while (i < nMsgs) {
      val msg = c.receive.asInstanceOf[TextMessage]
      if (msg != null) {
        val s = msg.getText
        s should  be(dataFormat.format(i))
        System.out.println("Received: " + i)
        i += 1
      }
    }
    connection.close
  }


}