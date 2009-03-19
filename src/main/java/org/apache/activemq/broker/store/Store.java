package org.apache.activemq.broker.store;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.CancellationException;

import org.apache.activemq.flow.ISourceController;
import org.apache.activemq.protobuf.AsciiBuffer;
import org.apache.activemq.protobuf.Buffer;

import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicBoolean;

/**
 * Interface to persistently store and access data needed by the messaging
 * system.
 * 
 */
public interface Store {

    public interface RecordKey {
    }
    
    /**
     * This interface allows you to query and update the Store.
     * 
     * This interface should only be called within the context of a transaction
     * controlled by the {@link Store#execute(Callback)} mehtod.
     * 
     */
    public interface Session {

        // Message related methods.
        public RecordKey messageAdd(AsciiBuffer messageId, Buffer message);

        public RecordKey messageGetKey(AsciiBuffer messageId);

        public Buffer messageGet(RecordKey key);

        // Message Chunking related methods.
        public RecordKey messageChunkOpen(AsciiBuffer messageId, Buffer txid, Buffer message);

        public void messageChunkAdd(RecordKey key, Buffer message);

        public void messageChunkClose(RecordKey key);

        public Buffer messageChunkGet(RecordKey key, int offset, int max);

        // / Queue related methods.
        public Iterator<AsciiBuffer> queueList(AsciiBuffer first, int max);

        public void queueAdd(AsciiBuffer queue);

        public boolean queueRemove(AsciiBuffer queue);

        public void queueAddMessage(AsciiBuffer queue, RecordKey key);

        public void queueRemoveMessage(AsciiBuffer queue, RecordKey key);

        public Iterator<RecordKey> queueListMessagesQueue(AsciiBuffer queue, RecordKey firstRecord, int max);

        // We could use this to associate additional data to a message on a
        // queue like
        // which consumer a message has been dispatched to.
        public void queueSetMessageAttachment(AsciiBuffer queue, RecordKey key, Buffer attachment);

        public Buffer queueGetMessageAttachment(AsciiBuffer queue, RecordKey key);

        // / Simple Key Value related methods could come in handy to store misc
        // data.
        public Iterator<AsciiBuffer> mapList(AsciiBuffer first, int max);

        public Buffer mapSet(AsciiBuffer map, Buffer key, Buffer value);

        public Buffer mapGet(AsciiBuffer map, Buffer key);

        public Buffer mapRemove(AsciiBuffer map, Buffer key);

        public Iterator<Buffer> mapListKeys(AsciiBuffer map, Buffer first, int max);

    }

    /**
     * This interface is used to execute transacted code.
     * 
     * It is used by the {@link Store#execute(Callback)} method, often as
     * anonymous class.
     */
    public interface Operation {

        /**
         * Gets called by the {@link Store#add(Operation, ISourceController, boolean)} method within a
         * transactional context. If any exception is thrown including Runtime
         * exception, the transaction is rolled back.
         * 
         * @param session
         *            provides you access to read and update the persistent
         *            data.
         * @return the result of the CallableCallback
         * @throws CancellationException
         *             if the operation has been canceled. If this is thrown, 
         *             the {@link #onCommit()} and {@link #onRollback()} methods will
         *             not be called. 
         * @throws Exception
         *             if an system error occured while executing the operations.
         * @throws RuntimeException
         *             if an system error occured while executing the operations.
         */
        public void execute(Session session) throws CancellationException, Exception, RuntimeException;
        
        /**
         * Returns true if this operation can be delayed. This is useful in cases 
         * where external events can negate the need to execute the operation. The delay
         * interval is not guaranteed to be honored, if subsequent events or other 
         * store flush policy/criteria requires a flush of subsequent events. 
         * 
         * @return True if the operation can be delayed. 
         */
        public boolean isDelayable();
        
        /**
         * Attempts to cancel the store operation. Returns true if the operation
         * could be canceled or false if the operation was already executed by the 
         * store. 
         * 
         * @return true if the operation could be canceled
         */
        public boolean cancel();
        
        /**
         * Returns the size to be used when calculating how much space this operation
         * takes on the store processing queue. 
         * 
         * @return The limiter size to be used. 
         */
        public long getLimiterSize();
        
        /**
         * Called after {@link #execute(Session)} is called and the the operation has been committed. 
         */
        public void onCommit();
        
        /**
         * Called after {@link #execute(Session)} is called and the the operation has been rolled back. 
         */
        public void onRollback(Throwable error);
    }
    
    /**
     * This is a convenience base class that can be used to implement Operations.  
     * It handles operation cancellation for you. 
     */
    public abstract class OperationBase implements Operation {
        final private AtomicBoolean executePending = new AtomicBoolean(true);
        
        public boolean cancel() {
            return executePending.compareAndSet(true, false);
        }

        public void execute(Session session) throws CancellationException {
            if( executePending.compareAndSet(true, false) ) {
                doExcecute(session);
            } else {
                throw new CancellationException();
            }
        }

        abstract protected void doExcecute(Session session);
        
        public long getLimiterSize() {
            return 0;
        }

        public boolean isDelayable() {
            return false;
        }

        public void onCommit() {
        }

        public void onRollback() {
        }
    }

    /**
     * Executes user supplied {@link Operation}. If the {@link Operation} does not
     * throw any Exceptions, all updates to the store are committed, otherwise
     * they are rolled back. Any exceptions thrown by the {@link Operation} are
     * propagated by this method.
     * 
     * If limiter space on the store processing queue is exceeded, the controller will be
     * blocked. 
     * 
     * If this method is called with flush set to <code>false</false> there is no 
     * guarantee made about when the operation will be executed. If <code>flush</code> is 
     * <code>true</code> and {@link Operation#isDelayable()} is also <code>true</code>
     * then an attempt will be made to execute the event at the {@link Store}'s configured
     * delay interval. 
     * 
     * @param op The operation to execute
     * @param flush Whether or not this operation needs immediate processing. 
     * @param controller the source of the operation. 
     */
    public void add(Operation op, ISourceController<?> controller, boolean flush);

}
