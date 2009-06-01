package org.apache.activemq.filter;

public class FilterException extends Exception {

    private static final long serialVersionUID = -6892363158919485507L;

    public FilterException() {
        super();
    }

    public FilterException(String message, Throwable cause) {
        super(message, cause);
    }

    public FilterException(String message) {
        super(message);
    }

    public FilterException(Throwable cause) {
        super(cause);
    }
    
}
