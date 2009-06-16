package org.apache.activemq.apollo.jaxb;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.activemq.broker.store.Store;
import org.apache.activemq.broker.store.memory.MemoryStore;

@XmlRootElement(name="memory-store")
@XmlAccessorType(XmlAccessType.FIELD)
public class MemoryStoreXml implements StoreXml {

	public Store createStore() {
		MemoryStore rc = new MemoryStore();
		return rc;
	}

}
