package org.apache.activemq.broker.store.hawtdb.store;

/**
* Created by IntelliJ IDEA.
* User: chirino
* Date: May 19, 2010
* Time: 4:49:17 PM
* To change this template use File | Settings | File Templates.
*/
public class FatalStoreException extends RuntimeException {
    private static final long serialVersionUID = 1122460895970375737L;

    public FatalStoreException() {
    }

    public FatalStoreException(String message, Throwable cause) {
        super(message, cause);
    }

    public FatalStoreException(String message) {
        super(message);
    }

    public FatalStoreException(Throwable cause) {
        super(cause);
    }
}
