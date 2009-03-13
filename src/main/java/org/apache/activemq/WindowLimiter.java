/**
 * 
 */
package org.apache.activemq;

import org.apache.activemq.broker.MessageDelivery;
import org.apache.activemq.flow.Flow;
import org.apache.activemq.flow.SizeLimiter;

public class WindowLimiter<E> extends SizeLimiter<E>  {
        final Flow flow;
        final boolean clientMode;
        private int available;

        public WindowLimiter(boolean clientMode, Flow flow, int capacity, int resumeThreshold) {
            super(capacity, resumeThreshold);
            this.clientMode = clientMode;
            this.flow = flow;
        }

        public void reserve(E elem) {
            super.reserve(elem);
//            if (!clientMode) {
//                 System.out.println(name + " Reserved " + this);
//            }
        }

        public void releaseReserved(E elem) {
            super.reserve(elem);
//            if (!clientMode) {
//                System.out.println(name + " Released Reserved " + this);
//            }
        }

        protected void remove(int size) {
            super.remove(size);
            if (!clientMode) {
                available += size;
                if (available >= capacity - resumeThreshold) {
                    sendCredit(available);
                    available = 0;
                }
            }
        }

        protected void sendCredit(int credit) {
            throw new UnsupportedOperationException("Please override this method to provide and implemenation.");
        }

        public void onProtocolCredit(int credit) {
            remove(credit);
        }

        public int getElementSize(MessageDelivery m) {
            return m.getFlowLimiterSize();
        }
    }