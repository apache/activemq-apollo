package org.apache.activemq.broker;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

import org.apache.activemq.protobuf.AsciiBuffer;

public class TopicDomain implements Domain {
    
    final HashMap<AsciiBuffer, ArrayList<DeliveryTarget>> topicsTargets = new HashMap<AsciiBuffer, ArrayList<DeliveryTarget>>();

    public void add(AsciiBuffer name, Object queue) {
    }
    public Object remove(AsciiBuffer name) {
        return null;
    }

    public void bind(AsciiBuffer name, DeliveryTarget target) {
        ArrayList<DeliveryTarget> targets = topicsTargets.get(name);
        if (targets == null) {
            targets = new ArrayList<DeliveryTarget>();
            topicsTargets.put(name, targets);
        }
        targets.add(target);
    }

    public Collection<DeliveryTarget> route(MessageDelivery name) {
        return topicsTargets.get(name);
    }

}
