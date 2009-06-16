package org.apache.activemq.apollo.jaxb;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.activemq.broker.store.Store;

public interface StoreXml {

	Store createStore();

}
