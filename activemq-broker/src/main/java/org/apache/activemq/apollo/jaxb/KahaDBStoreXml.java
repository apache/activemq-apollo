package org.apache.activemq.apollo.jaxb;

import java.io.File;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.activemq.broker.store.Store;
import org.apache.activemq.broker.store.kahadb.KahaDBStore;

@XmlRootElement(name="kahadb-store")
@XmlAccessorType(XmlAccessType.FIELD)
public class KahaDBStoreXml extends StoreXml {

	@XmlAttribute(name="checkpoint-interval", required=false)
	private Long checkpointInterval;
	@XmlAttribute(name="cleanup-interval", required=false)
	private Long cleanupInterval;
	@XmlAttribute(name="purge-on-startup", required=false)
	private Boolean purgeOnStartup;
	@XmlAttribute(name="index-write-async", required=false)
	private Boolean indexWriteAsync;
	@XmlAttribute(name="journal-disk-syncs", required=false)
	private Boolean journalDiskSyncs;
	@XmlAttribute(name="fail-if-database-is-locked", required=false)
	private Boolean failIfDatabaseIsLocked;
	@XmlAttribute(name="index-write-batch-size", required=false)
	private Integer indexWriteBatchSize;
	@XmlAttribute(name="journal-max-file-length", required=false)
	private Integer journalMaxFileLength;
	@XmlAttribute(name="directory", required=false)
	private File directory;

	public Store createStore() {
		KahaDBStore rc = new KahaDBStore();
		if( checkpointInterval!=null )
			rc.setCheckpointInterval(checkpointInterval);
		if( cleanupInterval!=null )
			rc.setCleanupInterval(cleanupInterval);
		if( purgeOnStartup!=null )
			rc.setDeleteAllMessages(purgeOnStartup);
		if( indexWriteAsync!=null )
			rc.setEnableIndexWriteAsync(indexWriteAsync);
		if( journalDiskSyncs!=null )
			rc.setEnableJournalDiskSyncs(journalDiskSyncs);
		if( failIfDatabaseIsLocked!=null )
			rc.setFailIfDatabaseIsLocked(failIfDatabaseIsLocked);
		if( indexWriteBatchSize!=null )
			rc.setIndexWriteBatchSize(indexWriteBatchSize);
		if( journalMaxFileLength!=null )
			rc.setJournalMaxFileLength(journalMaxFileLength);
		if( directory!=null )
			rc.setStoreDirectory(directory);
		return rc;
	}

}
