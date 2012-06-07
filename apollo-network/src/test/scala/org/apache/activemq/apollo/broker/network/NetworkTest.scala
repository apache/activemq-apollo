package org.apache.activemq.apollo.broker.network

import org.scalatest.matchers.ShouldMatchers
import org.scalatest.BeforeAndAfterEach
import org.apache.activemq.apollo.broker.MultiBrokerTestSupport


class NetworkTest extends MultiBrokerTestSupport with ShouldMatchers with BeforeAndAfterEach {

  override def broker_config_uris = Array(
    "xml:classpath:apollo-network-1.xml",
    "xml:classpath:apollo-network-2.xml"
  )

  test("basics") {
    admins(0).broker should not be(null)
    val config = admins(0).broker.config
    admins(1).broker should not be(null)
  }

}