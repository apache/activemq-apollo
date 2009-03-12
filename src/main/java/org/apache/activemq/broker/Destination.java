package org.apache.activemq.broker;

import java.util.Collection;

import org.apache.activemq.protobuf.AsciiBuffer;

public interface Destination {

    AsciiBuffer getDomain();
    AsciiBuffer getName();
    Collection<Destination> getDestinations();
    
    public class SingleDestination implements Destination {

        private AsciiBuffer domain;
        private AsciiBuffer name;
        
        public SingleDestination() {
        }
        public SingleDestination(AsciiBuffer domain, AsciiBuffer name) {
            setDomain(domain);
            setName(name);
        }
        public SingleDestination(String domain, String name) {
            setDomain(domain);
            setName(name);
        }

        public Collection<Destination> getDestinations() {
            return null;
        }

        public AsciiBuffer getDomain() {
            return domain;
        }

        public AsciiBuffer getName() {
            return name;
        }
        public void setDomain(AsciiBuffer domain) {
            this.domain = domain;
        }
        public void setName(AsciiBuffer name) {
            this.name = name;
        }
        
        private void setName(String name) {
            setName(new AsciiBuffer(name));
        }
        private void setDomain(String domain) {
            setDomain(new AsciiBuffer(domain));
        }
    }
    
    public class MultiDestination implements Destination {

        private Collection<Destination> destinations;

        public MultiDestination() {
        }

        public MultiDestination(Collection<Destination> destinations) {
            this.destinations=destinations;
        }

        public Collection<Destination> getDestinations() {
            return destinations;
        }
        
        public void setDestinations(Collection<Destination> destinations) {
            this.destinations = destinations;
        }

        public AsciiBuffer getDomain() {
            return null;
        }

        public AsciiBuffer getName() {
            return null;
        }

    }
    
}
