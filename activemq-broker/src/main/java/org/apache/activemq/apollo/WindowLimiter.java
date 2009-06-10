/**
 * 
 */
package org.apache.activemq.apollo;

import org.apache.activemq.apollo.broker.MessageDelivery;
import org.apache.activemq.flow.Flow;
import org.apache.activemq.flow.SizeLimiter;

public class WindowLimiter<E extends MessageDelivery> extends SizeLimiter<E>  {
        final Flow flow;
        final boolean clientMode;
        private int available;

        public WindowLimiter(boolean clientMode, Flow flow, int capacity, int resumeThreshold) {
            super(capacity, resumeThreshold);
            this.clientMode = clientMode;
            this.flow = flow;
        }

        @Override
        public void reserve(E elem) {
            super.reserve(elem);
//            if (!clientMode) {
//                 System.out.println(name + " Reserved " + this);
//            }
        }

        /*
        public void releaseReserved(E elem) {
            super.reserve(elem);
//            if (!clientMode) {
//                System.out.println(name + " Released Reserved " + this);
//            }
        }*/
        
        @Override
        public void remove(int count, long size) {
            super.remove(count, size);
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
            remove(1, credit);
        }

        public int getElementSize(E m) {
            return m.getFlowLimiterSize();
        }
    }