/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hawtdb.internal.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Map.Entry;

import org.apache.activemq.util.TreeMap;
import org.apache.activemq.util.TreeMap.TreeEntry;

/**
 * Tracks numeric ranges.  Handy for keeping track of things like allocation or free lists.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
final public class Ranges implements Serializable, Iterable<Ranges.Range> {
    private static final long serialVersionUID = 8340484139329633582L;

    final public static class Range implements Serializable {
        private static final long serialVersionUID = -4904483630105365841L;
        
        public int start;
        public int end;

        public Range(int start, int end) {
            this.start = start;
            this.end = end;
        }

        public int size() {
            return end - start;
        }
                
        @Override
        public String toString() {
            if( start == end-1 ) {
                return Integer.toString(start);
            }
            return start+"-"+(end-1);
        }
        
        @Override
        public boolean equals(Object obj) {
            if( obj == this ) {
                return true;
            }
            if( obj == null || obj.getClass()!=Range.class ) {
                return false;
            }
            Range r = (Range)obj;
            return start == r.start && end==r.end; 
        }
        
        @Override
        public int hashCode() {
            return start*77+end;
        }

        public boolean contains(int value) {
            return start <= value && value < end;
        }
    }

    private final TreeMap<Integer, Range> ranges = new TreeMap<Integer, Range>();

    public void add(int start) {
        add(start, 1);
    }
    
    public void add(int start, int length) {
        int end = start+length;
        
        // look for entries starting from the end of the add range.
        TreeEntry<Integer, Range> entry = ranges.floorEntry(end);
        if( entry!=null ) {
            while( entry!=null) {
                Range range = entry.getValue();
                TreeEntry<Integer, Range> curr = entry;
                entry = entry.previous();
                
                // If tail of the range is not in the add range.
                if( range.end < start ) {
                    // we are done..
                    break;
                }
                
                // if the end of the range is not in the add range.
                if( end < range.end  ) {
                    // extend the length out..
                    end = range.end;
                }

                if( start < range.start ) {
                    // if the front of the range is in the add range.
                    // just remove it..
                    ranges.removeEntry(curr);
                } else {
                    // The front is not in the add range...
                    // Then resize.. and we are done
                    range.end = end;
                    return;
                }
            }
        }
        
        // put the new range in.
        ranges.put(start, range(start, end));
    }    
    
    public void remove(int start) {
        remove(start, 1);
    }
    
    public void remove(int start, int length) {
        int end = start+length;
        
        // look for entries starting from the end of the remove range.
        TreeEntry<Integer, Range> entry = ranges.lowerEntry(end);
        while( entry!=null) {
            Range range = entry.getValue();
            TreeEntry<Integer, Range> curr = entry;
            entry = entry.previous();
            
            // If tail of the range is not in the remove range.
            if( range.end <= start ) {
                // we are done..
                break;
            }
            
            // if the end if the range is not in the remove range.
            if( end < range.end  ) {
                // Then we need to add back the tail part.
                ranges.put(end, range(end, range.end));
            }

            if( start <= range.start ) {
                // if the front of the range is in the remove range.
                // just remove it..
                ranges.removeEntry(curr);
                
            } else {
                // The front is not in the remove range...
                // Then resize.. and we are done
                range.end = start;
                break;
            }
        }
    }

    public boolean contains(int value) {
        TreeEntry<Integer, Range> entry = ranges.floorEntry(value);
        if( entry == null ) {
            return false;
        }
        return entry.getValue().contains(value);
    }

    
    public void clear() {
        ranges.clear();
    }

    public void copy(Ranges source) {
        ranges.clear();
        for (Entry<Integer, Range> entry : source.ranges.entrySet()) {
            Range value = entry.getValue();
            ranges.put(entry.getKey(), range(value.start, value.end));
        }
    }
    
    public int size() {
        int rc=0;
        TreeEntry<Integer, Range> entry = ranges.firstEntry();
        while(entry!=null) {
            rc += entry.getValue().size();
            entry = entry.next();
        }
        return rc;
    }

    
    static public Range range(int start, int end) {
        return new Range(start, end);
    }
    
    public ArrayList<Range> toArrayList() {
        return new ArrayList<Range>(ranges.values());
    }
    
    @Override
    public String toString() {
        StringBuilder sb  = new StringBuilder(20+(10*ranges.size()));
        sb.append("[ ");
        
        boolean first=true;
        for (Range r : this) {
            if( !first ) {
                sb.append(", ");
            }
            first=false;
            sb.append(r);
        }
        
        sb.append(" ]");
        return sb.toString();
    }
    
    public Iterator<Range> iterator() {
        return ranges.values().iterator();
    }

    public Iterator<Range> iteratorNotInRange(final Range mask) {
        
        return new Iterator<Range>() {
            
            Iterator<Range> iter = ranges.values().iterator();
            Range last = new Range(mask.start, mask.start);
            Range next = null;

            public boolean hasNext() {
                if( next==null ) {
                    while( last.end < mask.end && iter.hasNext() ) {
                        Range r = iter.next();
                        
                        // skip over the initial ranges not within the mask
                        if( r.end < last.end ) {
                            continue;
                        }
                        
                        // Handle the case where a range straddles the mask start position 
                        if( r.start < last.end ) {
                            // extend the last range out so that the next one starts at
                            // the end of the range.
                            last = new Range(last.start, r.end);
                            continue;
                        }

                        if( r.start < mask.end ) {
                            next = new Range(last.end, r.start);
                        } else {
                            next = new Range(last.end, mask.end);
                        }
                        break;
                    }
                }
                return next!=null;
            }

            public Range next() {
                if( !hasNext() ) {
                    throw new NoSuchElementException();
                }
                last = next;
                next=null;
                return last;
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
    
    static private final class ValueIterator implements Iterator<Integer>, Iterable<Integer> {
        Iterator<Range> ranges;
        Range range;
        Integer next;
        int last;

        private ValueIterator(Iterator<Range> t) {
            ranges = t;
        }

        public boolean hasNext() {
            if( next==null ) {
                if( range == null ) {
                    if( ranges.hasNext() ) {
                        range = ranges.next();
                        next = range.start;
                    } else {
                        return false;
                    }
                } else {
                    next = last+1;
                }
                if( next == (range.end-1) ) {
                    range=null;
                }
            }
            return next!=null;
        }

        public Integer next() {
            if( !hasNext() ) {
                throw new NoSuchElementException();
            }
            last = next;
            next=null;
            return last;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }

        public Iterator<Integer> iterator() {
            return this;
        }
    }    
    
    public List<Integer> values() {
        ArrayList<Integer> rc = new ArrayList<Integer>();
        for (Integer i : new ValueIterator(iterator())) {
            rc.add(i);
        }
        return rc;
    }
    
    public Iterator<Integer> valueIterator() {
       return new ValueIterator(iterator()); 
    }

    public Iterator<Integer> valuesIteratorNotInRange(Range r) {
        return new ValueIterator(iteratorNotInRange(r)); 
    }

    public boolean isEmpty() {
        return ranges.isEmpty();
    }
    
}
