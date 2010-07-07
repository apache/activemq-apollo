package org.apache.activemq.broker.store.hawtdb.store;

/**
* Created by IntelliJ IDEA.
* User: chirino
* Date: May 19, 2010
* Time: 4:49:31 PM
* To change this template use File | Settings | File Templates.
*/
public class DuplicateKeyException extends Exception {
    private static final long serialVersionUID = -477567614452245482L;

    public DuplicateKeyException() {
    }

    public DuplicateKeyException(String message) {
        super(message);
    }

    public DuplicateKeyException(String message, Throwable cause) {
        super(message, cause);
    }

    public DuplicateKeyException(Throwable cause) {
        super(cause);
    }
}
