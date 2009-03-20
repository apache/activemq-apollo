package org.apache.activemq.broker.store;

import java.util.Iterator;

import org.apache.activemq.protobuf.AsciiBuffer;
import org.apache.activemq.protobuf.Buffer;


/**
 * Interface to persistently store and access data needed by the messaging
 * system.
 */
public interface Store {

    /**
     * This interface is used to execute transacted code.
     * 
     * It is used by the {@link Store#execute(Callback)} method, often as
     * anonymous class.
     */
    public interface Callback<R, T extends Exception> {

        /**
         * Gets called by the {@link Store#execute(Callback)} method
         * within a transactional context. If any exception is thrown including
         * Runtime exception, the transaction is rolled back.
         * 
         * @param session
         *            provides you access to read and update the persistent
         *            data.
         * @return the result of the Callback
         * @throws T
         *            if an system error occured while executing the
         *            operations.
         */
        public R execute(Session session) throws T;
    }
    
    /**
     * Convenience class which allows you to implement {@link Callback} classes which do not return a value.
     */
    public abstract class VoidCallback <T extends Exception> implements Callback<Object, T> {
        
        /**
         * Gets called by the {@link Store#execute(VoidCallback)} method within a transactional context.  
         * If any exception is thrown including Runtime exception, the transaction is rolled back.
         * 
         * @param session provides you access to read and update the persistent data.
         * @throws T if an error occurs and the transaction should get rolled back
         */
        abstract public void run(Session session) throws T;
        
        final public Object execute(Session session) throws T {
            run(session);
            return null;
        }
    }

    /**
     * Executes user supplied {@link Callback}.  If the {@link Callback} does not throw
     * any Exceptions, all updates to the store are committed to the store as a single 
     * unit of work, otherwise they are rolled back. 
     * 
     * When this method returns, the transaction may be buffered by the Store implementation
     * it increase performance throughput.  The onFlush parameter can be used to know when
     * the transaction does get flushed is guaranteed to not be lost if a system crash occurs.
     * 
     * You can force the flushing of all previously buffered transactions using the {@link #flush} method.
     * 
     * Any exceptions thrown by the  {@link Callback} are propagated by this method.
     * 
     * @param <T>
     * @param closure
     * @param onFlush if not null, it's {@link Runnable#run()} method is called once he transaction has been store on disk.
     */
    public <R, T extends Exception> R execute(Callback<R,T> callback, Runnable onFlush) throws T;

    /**
     * Flushes all committed buffered transactions.
     */
    public void flush();


    interface RecordKey {
        
    }
    
    /**
     * This interface allows you to query and update the Store.
     * 
     * This interface should only be called within the context of a transaction
     * controlled by the {@link Store#execute(Callback)} mehtod.
     * 
     */
    public interface Session {

        public class DuplicateKeyException extends Exception {
            private static final long serialVersionUID = 1L;

            public DuplicateKeyException(String message) {
                super(message);
            }
        }

        public class QueueNotFoundException extends Exception {
            private static final long serialVersionUID = 1L;

            public QueueNotFoundException(String message) {
                super(message);
            }
        }
        
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
        public Iterator<AsciiBuffer> queueList(AsciiBuffer first);
        public void queueAdd(AsciiBuffer queue);
        public boolean queueRemove(AsciiBuffer queue);
        public void queueAddMessage(AsciiBuffer queue, RecordKey key, Buffer attachment) throws QueueNotFoundException, DuplicateKeyException;
        public void queueRemoveMessage(AsciiBuffer queue, RecordKey key) throws QueueNotFoundException;
        public Iterator<Buffer> queueListMessagesQueue(AsciiBuffer queue, RecordKey firstRecord, int max);

        // We could use this to associate additional data to a message on a
        // queue like which consumer a message has been dispatched to.
        // public void queueSetMessageAttachment(AsciiBuffer queue, RecordKey
        // key, Buffer attachment) throws QueueNotFoundException;

        // public Buffer queueGetMessageAttachment(AsciiBuffer queue, RecordKey
        // key) throws QueueNotFoundException;

        // / Simple Key Value related methods could come in handy to store misc
        // data.
        public Iterator<AsciiBuffer> mapList(AsciiBuffer first, int max);
        public Buffer mapSet(AsciiBuffer map, Buffer key, Buffer value);
        public Buffer mapGet(AsciiBuffer map, Buffer key);
        public Buffer mapRemove(AsciiBuffer map, Buffer key);
        public Iterator<Buffer> mapListKeys(AsciiBuffer map, Buffer first, int max);

    }
}
