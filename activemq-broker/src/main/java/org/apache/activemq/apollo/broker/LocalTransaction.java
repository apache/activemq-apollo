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
package org.apache.activemq.apollo.broker;

import java.io.IOException;
import java.util.HashSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.transaction.xa.XAException;

import org.apache.activemq.util.FutureListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * LocalTransaction
 * <p>
 * Description:
 * </p>
 * 
 * @author cmacnaug
 * @version 1.0
 */
public class LocalTransaction extends Transaction {

    private static final Log LOG = LogFactory.getLog(LocalTransaction.class);
    
//    TODO:
//    LocalTransaction(TransactionManager manager, long tid, IQueue<Long, TxOp> opQueue) {
//        super(manager, tid, opQueue);
//    }
//
//    /*
//     * (non-Javadoc)
//     *
//     * @see org.apache.activemq.apollo.broker.Transaction#commit(boolean)
//     */
//    @Override
//    public void commit(boolean onePhase, final TransactionListener listener) throws XAException, IOException {
//        if (LOG.isDebugEnabled()) {
//            LOG.debug("commit: " + this);
//        }
//
//        synchronized(this)
//        {
//            // Get ready for commit.
//            try {
//                prePrepare();
//            } catch (XAException e) {
//                throw e;
//            } catch (Throwable e) {
//                LOG.warn("COMMIT FAILED: ", e);
//                rollback(null);
//                // Let them know we rolled back.
//                XAException xae = new XAException("COMMIT FAILED: Transaction rolled back.");
//                xae.errorCode = XAException.XA_RBOTHER;
//                xae.initCause(e);
//                throw xae;
//            }
//
//            //Add the listener for commit
//            if(listeners == null)
//            {
//                listeners = new HashSet<TransactionListener>();
//            }
//            listeners.add(listener);
//
//            //Update the transaction state to committed,
//            //and on complete process the commit:
//            setState(COMMITED_STATE, new FutureListener<Object>()
//            {
//                public void onFutureComplete(Future<? extends Object> dbCommitResult) {
//                    try {
//                        fireAfterCommit();
//                        startTransactionProcessor();
//                    } catch (InterruptedException e) {
//                        //Shouldn't happen
//                        LOG.warn(new AssertionError(e));
//                    } catch (ExecutionException e) {
//                        LOG.warn("COMMIT FAILED: ", e);
//                    }
//                    catch (Exception e)
//                    {
//                    }
//                }
//            });
//        }
//    }
//
//
//    public int prepare(TransactionListener listener) throws XAException {
//        XAException xae = new XAException("Prepare not implemented on Local Transactions.");
//        xae.errorCode = XAException.XAER_RMERR;
//        throw xae;
//    }
//
//    /*
//     * (non-Javadoc)
//     *
//     * @see org.apache.activemq.apollo.broker.Transaction#rollback()
//     */
//    @Override
//    public void rollback(TransactionListener listener) throws XAException, IOException {
//        // TODO Auto-generated method stub
//        throw new UnsupportedOperationException("Not yet implemnted");
//    }
//
//    /* (non-Javadoc)
//     * @see org.apache.activemq.apollo.broker.Transaction#getType()
//     */
//    @Override
//    public byte getType() {
//        return TYPE_LOCAL;
//    }

}
