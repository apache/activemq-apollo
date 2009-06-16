package org.apache.activemq.apollo.jaxb;

import javax.xml.bind.annotation.XmlType;

import org.apache.activemq.broker.store.Store;

@XmlType(name = "storeType")
public abstract class StoreXml {

	abstract Store createStore();

}
