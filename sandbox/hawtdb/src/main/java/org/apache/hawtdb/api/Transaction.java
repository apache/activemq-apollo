/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hawtdb.api;



/**
 * Provides transactional access to a {@link Paged} resource.
 * The transaction provides snapshot isolation.  The snapshot view
 * of the entire page file is obtained the first time you read data 
 * from a page.  Committed page updates from concurrent transactions will
 * not be visible to the snapshot.
 * 
 * The snapshot view of the page file is released once the transaction 
 * commits or is rolled back.  Avoid holding a snapshot view for a long time.
 * The page file cannot reclaim temporary processing space associated with
 * a snapshot and subsequent snapshots while the snapshot is in use.
 * 
 * Pages are optimistically updated, which means they are not locked for 
 * update.  Updating a page or committing the transaction may fail 
 * with an {@line OptimisticUpdateException} if another committed transaction
 * has updated the same page this transaction was trying to update.
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public interface Transaction extends Paged {
    
    /**
     * @return true if no updates have been performed by this transaction.
     */
    boolean isReadOnly();
    
    /**
     * @throws OptimisticUpdateException 
     *      is thrown if the update would conflict with a concurrent 
     *      updated performed by another thread.
     */
    void commit() throws OptimisticUpdateException;
    
    /**
     * 
     */
	void rollback();
	
}
