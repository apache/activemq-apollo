package org.apache.activemq.apollo

resSPI ::= SPI
    } catch {
      case e:Throwable =>
        e.printStackTrace
    }
  }

  def create(config:StoreDTO):Store = {
    if( config == null ) {
      return null
    }
    storesSPI.foreach { spi=>
      val rc = spi.create(config)
      if( rc!=null ) {
        return rc
      }
    }
    throw new IllegalArgumentException("Uknonwn store type: "+config.getClass)
  }


  def validate(config: StoreDTO, reporter:Reporter):ReporterLevel = {
    if( config == null ) {
      return INFO
    } else {
      storesSPI.foreach { spi=>
        val rc = spi.validate(config, reporter)
        if( rc!=null ) {
          return rc
        }
      }
    }
    reporter.report(ERROR, "Uknonwn store type: "+config.getClass)
    ERROR
  }

}