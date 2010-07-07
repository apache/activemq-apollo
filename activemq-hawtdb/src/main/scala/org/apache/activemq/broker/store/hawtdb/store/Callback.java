package org.apache.activemq.broker.store.hawtdb.store;

/**
 * This interface is used to execute transacted code.
 *
 */
public interface Callback<R, T extends Exception> {

    /**
     * @param session
     *            provides you access to read and update the persistent
     *            data.
     * @return the result of the Callback
     * @throws T
     *             if an system error occured while executing the
     *             operations.
     */
    public R execute(HawtDBSession session) throws T;
}
