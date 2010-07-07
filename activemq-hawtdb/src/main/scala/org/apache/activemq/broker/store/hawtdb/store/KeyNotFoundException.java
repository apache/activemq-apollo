package org.apache.activemq.broker.store.hawtdb.store;

/**
* Created by IntelliJ IDEA.
* User: chirino
* Date: May 19, 2010
* Time: 4:49:45 PM
* To change this template use File | Settings | File Templates.
*/
public class KeyNotFoundException extends Exception {
    private static final long serialVersionUID = -2570252319033659546L;

    public KeyNotFoundException() {
        super();
    }

    public KeyNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public KeyNotFoundException(String message) {
        super(message);
    }

    public KeyNotFoundException(Throwable cause) {
        super(cause);
    }
}
