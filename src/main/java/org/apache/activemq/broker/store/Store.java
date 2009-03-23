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
        public static class MessageRecord {
            Long key;
            AsciiBuffer messageId;
            AsciiBuffer encoding;
            Buffer buffer;
            Long streamKey;
            
            public Long getKey() {
                return key;
            }
            public void setKey(Long key) {
                this.key = key;
            }
            public AsciiBuffer getMessageId() {
                return messageId;
            }
            public void setMessageId(AsciiBuffer messageId) {
                this.messageId = messageId;
            }
            public AsciiBuffer getEncoding() {
                return encoding;
            }
            public void setEncoding(AsciiBuffer encoding) {
                this.encoding = encoding;
            }
            public Buffer getBuffer() {
                return buffer;
            }
            public void setBuffer(Buffer buffer) {
                this.buffer = buffer;
            }
            public Long getStreamKey() {
                return streamKey;
            }
            public void setStreamKey(Long stream) {
                this.streamKey = stream;
            }
        }

        public Long messageAdd(MessageRecord message);
        public Long messageGetKey(AsciiBuffer messageId);
        public MessageRecord messageGetRecord(Long key);

        public Long streamOpen();
        public void streamWrite(Long key, Buffer message);
        public void streamClose(Long key);
        public Buffer streamRead(Long key, int offset, int max);
        public boolean streamRemove(Long key);

        // Transaction related methods.
        public Iterator<AsciiBuffer> transactionList(AsciiBuffer first, int max);
        public void transactionAdd(AsciiBuffer txid);
        public void transactionAddMessage(AsciiBuffer txid, Long messageKey);
        public void transactionRemoveMessage(AsciiBuffer txid, AsciiBuffer queueName, Long messageKey);
        public boolean transactionCommit(AsciiBuffer txid);
        public boolean transactionRollback(AsciiBuffer txid);
        
        // Queue related methods.
        public Iterator<AsciiBuffer> queueList(AsciiBuffer firstQueueName, int max);
        public void queueAdd(AsciiBuffer queueName);
        public boolean queueRemove(AsciiBuffer queueName);
        
        public static class QueueRecord {
            Long queueKey;
            Long messageKey;
            Buffer attachment;
            
            public Long getQueueKey() {
                return queueKey;
            }
            public void setQueueKey(Long queueKey) {
                this.queueKey = queueKey;
            }
            public Long getMessageKey() {
                return messageKey;
            }
            public void setMessageKey(Long messageKey) {
                this.messageKey = messageKey;
            }
            public Buffer getAttachment() {
                return attachment;
            }
            public void setAttachment(Buffer attachment) {
                this.attachment = attachment;
            }
        }
        public Long queueAddMessage(AsciiBuffer queueName, QueueRecord record) throws QueueNotFoundException;
        public void queueRemoveMessage(AsciiBuffer queueName, Long queueKey) throws QueueNotFoundException;
        public Iterator<QueueRecord> queueListMessagesQueue(AsciiBuffer queueName, Long firstQueueKey, int max);

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
