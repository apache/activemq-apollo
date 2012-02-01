package org.apache.activemq.apollo.broker

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait BrokerAware {

  def set_broker(value:Broker):Unit

}