package org.apache.activemq.apollo.broker.osgi

import org.apache.activemq.apollo.broker.web.{WebServerFactory, WebServer}
import org.apache.activemq.apollo.util.{Reporter, ReporterLevel}
import org.apache.activemq.apollo.broker.Broker
import org.fusesource.hawtdispatch._
import org.apache.activemq.apollo.dto.WebAdminDTO

import BrokerService._
import org.osgi.framework.{ServiceReference, BundleContext}
import org.osgi.util.tracker.{ServiceTrackerCustomizer, ServiceTracker}
import org.ops4j.pax.web.service.spi.WarManager

object OsgiWebServerFactory extends WebServerFactory.Provider with WebServer {

  var war_manager:WarManager = _
  var started_config:WebAdminDTO = null

  val war_manager_tracker = new ServiceTracker(context, classOf[WarManager].getName, new ServiceTrackerCustomizer {

    def addingService(reference: ServiceReference): AnyRef = OsgiWebServerFactory.synchronized {
      war_manager = context.getService(reference).asInstanceOf[WarManager]
      if( started_config!=null ) {
        war_manager.start(context.getBundle.getBundleId, started_config.prefix )
      }
      war_manager
    }

    def removedService(reference: ServiceReference, service: AnyRef): Unit = OsgiWebServerFactory.synchronized {
      context.ungetService(reference)
      war_manager = null
    }

    def modifiedService(reference: ServiceReference, service: AnyRef): Unit = {
    }
  })
  war_manager_tracker.open();

  def create(broker:Broker): WebServer = this

  def validate(config: WebAdminDTO, reporter: Reporter): ReporterLevel.ReporterLevel = OsgiWebServerFactory.synchronized {
    import ReporterLevel._
    return INFO
  }

  def start: Unit = start(NOOP)
  def stop: Unit = stop(NOOP)

  def start(onComplete: Runnable): Unit = Broker.BLOCKABLE_THREAD_POOL {
    OsgiWebServerFactory.synchronized {
      started_config = broker.config.web_admin
      if( war_manager!=null ) {
        war_manager.start(context.getBundle.getBundleId, started_config.prefix )
      }
    }
    onComplete.run
  }
  def stop(onComplete: Runnable): Unit = Broker.BLOCKABLE_THREAD_POOL {
    OsgiWebServerFactory.synchronized {
      if( war_manager!=null ) {
        war_manager.stop(context.getBundle.getBundleId)
      }
      started_config = null
    }
    onComplete.run
  }

}

