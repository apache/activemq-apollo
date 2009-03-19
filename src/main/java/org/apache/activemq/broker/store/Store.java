package org.apache.activemq.broker.store;

import java.util.Iterator;

import org.apache.activemq.protobuf.AsciiBuffer;
import org.apache.activemq.protobuf.Buffer;

/**
 * Interface to persistently store and access data needed by the messaging system. 
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
        
        /// Queue related methods.
        public Iterator<AsciiBuffer> queueList(AsciiBuffer first, int max);
        public void queueAdd(AsciiBuffer queue);
        public boolean queueRemove(AsciiBuffer queue);
        
        public void queueAddMessage(AsciiBuffer queue, RecordKey key);
        public void queueRemoveMessage(AsciiBuffer queue, RecordKey key);
        public Iterator<RecordKey> queueListMessagesQueue(AsciiBuffer queue, RecordKey firstRecord, int max);
        
        // We could use this to associate additional data to a message on a queue like 
        // which consumer a message has been dispatched to.
        public void queueSetMessageAttachment(AsciiBuffer queue, RecordKey key, Buffer attachment);
        public Buffer queueGetMessageAttachment(AsciiBuffer queue, RecordKey key);
                
        /// Simple Key Value related methods could come in hand to store misc data.
        public Iterator<AsciiBuffer> mapList(AsciiBuffer first, int max);
        public Buffer mapSet(AsciiBuffer map, Buffer key, Buffer value);
        public Buffer mapGet(AsciiBuffer map, Buffer key);
        public Buffer mapRemove(AsciiBuffer map, Buffer key);
        public Iterator<Buffer> mapListKeys(AsciiBuffer map, Buffer first, int max);

    }
    
    /**
     *  
     * This interface is used to execute transacted code which returns a result.  
     * 
     * It is used by the {@link Store#execute(Callback)} method, 
     * often as anonymous class.
     * 
     * @param <R> The type of result that the CallableCallback produces.
     * @param <T> The type of exceptions that CallableCallback will throw.
     */
    public interface Callback<R, T extends Throwable> {
        
        /**
         * Gets called by the {@link Store#execute(Callback)} method within a transactional context.  
         * If any exception is thrown including Runtime exception, the transaction is rolled back.
         * 
         * @param session provides you access to read and update the persistent data.
         * @return the result of the CallableCallback
         * @throws T if an error occurs and the transaction should get rolled back
         */
        public R execute(Session session) throws T;
    }
    
    /**
     * Convenience class which allows you to implement {@link Callback} classes which do not return a value.
     */
    public abstract class VoidCallback <T extends Throwable> implements Callback<Object, T> {
        
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
     * any Exceptions, all updates to the store are committed, otherwise they
     * are rolled back. Any exceptions thrown by the {@link Callback} are propagated by
     * this method.
     * 
     * @param <T>
     * @param closure
     */
    public <R, T extends Throwable> R execute(Callback<R,T> closure);
    
    
}
