package org.apache.activemq.dispatch.internal.simple;


public class IntegerCounter {
    
    int counter;

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        IntegerCounter other = (IntegerCounter) obj;
        if (counter != other.counter)
            return false;
        return true;
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + counter;
        return result;
    }

    public final int addAndGet(int delta) {
        counter+=delta;
        return counter;
    }

    public final int decrementAndGet() {
        return --counter;
    }

    public final int get() {
        return counter;
    }

    public final int getAndAdd(int delta) {
        int rc = counter;
        counter += delta;
        return rc;
    }

    public final int getAndDecrement() {
        int rc = counter;
        counter --;
        return rc;
    }

    public final int getAndIncrement() {
        return counter++;
    }

    public final int getAndSet(int newValue) {
        int rc = counter;
        counter = newValue;
        return rc;
    }

    public final int incrementAndGet() {
        return ++counter;
    }

    public int intValue() {
        return counter;
    }

    public final void set(int newValue) {
        counter = newValue;
    }

    public String toString() {
        return Integer.toString(counter);
    }
    
}
