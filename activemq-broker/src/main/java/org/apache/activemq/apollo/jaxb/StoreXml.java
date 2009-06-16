package org.apache.activemq.apollo.jaxb;

import javax.xml.bind.annotation.XmlSeeAlso;

import org.apache.activemq.broker.store.Store;

@XmlSeeAlso( {MemoryStoreXml.class, KahaDBStoreXml.class} )
public interface StoreXml {

	Store createStore();

}
