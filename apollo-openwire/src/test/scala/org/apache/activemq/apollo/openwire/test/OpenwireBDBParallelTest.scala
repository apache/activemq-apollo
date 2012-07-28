package org.apache.activemq.apollo.openwire.test

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class OpenwireBDBParallelTest extends OpenwireParallelTest {
  override def broker_config_uri = "xml:classpath:apollo-openwire-bdb.xml"
}