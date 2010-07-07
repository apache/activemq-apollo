package org.apache.activemq.broker.store.hawtdb.store;

/**
 * Convenience class which allows you to implement {@link Callback} classes
 * which do not return a value.
 */
public abstract class VoidCallback<T extends Exception> implements Callback<Object, T> {

    /**
     *
     * @param session
     *            provides you access to read and update the persistent
     *            data.
     * @throws T
     *             if an error occurs and the transaction should get rolled
     *             back
     */
    abstract public void run(HawtDBSession session) throws T;

    final public Object execute(HawtDBSession session) throws T {
        run(session);
        return null;
    }
}
