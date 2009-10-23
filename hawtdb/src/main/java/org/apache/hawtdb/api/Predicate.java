/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hawtdb.api;

import java.util.List;

/**
 * Handy predicates for restricting the range of keys visited in a visitor.
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 * @param <Key>
 */
public interface Predicate<Key> {
    boolean isInterestedInKeysBetween(Key first, Key second);
    boolean isInterestedInKey(Key key);
    
    
    class OrPredicate<Key> implements Predicate<Key> {
        private final List<Predicate<Key>> conditions;

        public OrPredicate(List<Predicate<Key>> conditions) {
            this.conditions = conditions;
        }

        final public boolean isInterestedInKeysBetween(Key first, Key second) {
            for (Predicate<Key> condition : conditions) {
                if( condition.isInterestedInKeysBetween(first, second) ) {
                    return true;
                }
            }
            return false;
        }

        final public boolean isInterestedInKey(Key key) {
            for (Predicate<Key> condition : conditions) {
                if( condition.isInterestedInKey(key) ) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            boolean first=true;
            for (Predicate<Key> condition : conditions) {
                if( !first ) {
                    sb.append(" OR ");
                }
                first=false;
                sb.append("(");
                sb.append(condition);
                sb.append(")");
            }
            return sb.toString();
        }
    }

    class AndPredicate<Key> implements Predicate<Key> {
        private final List<Predicate<Key>> conditions;

        public AndPredicate(List<Predicate<Key>> conditions) {
            this.conditions = conditions;
        }

        final public boolean isInterestedInKeysBetween(Key first, Key second) {
            for (Predicate<Key> condition : conditions) {
                if( !condition.isInterestedInKeysBetween(first, second) ) {
                    return false;
                }
            }
            return true;
        }

        final public boolean isInterestedInKey(Key key) {
            for (Predicate<Key> condition : conditions) {
                if( !condition.isInterestedInKey(key) ) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            boolean first=true;
            for (Predicate<Key> condition : conditions) {
                if( !first ) {
                    sb.append(" AND ");
                }
                first=false;
                sb.append("(");
                sb.append(condition);
                sb.append(")");
            }
            return sb.toString();
        }
    }

    class BetweenPredicate<Key extends Comparable<? super Key>> implements Predicate<Key> {
        private final Key first;
        private final Key last;

        public BetweenPredicate(Key first, Key last) {
            this.first = first;
            this.last = last;
        }

        final public boolean isInterestedInKeysBetween(Key left, Key right) {
            return (right==null || right.compareTo(first)>=0)
                    && (left==null || left.compareTo(last)<0);
        }

        final public boolean isInterestedInKey(Key key) {
            return key.compareTo(first) >=0 && key.compareTo(last) <0;
        }

        @Override
        public String toString() {
            return first+" <= key < "+last;
        }
    }

    class GTPredicate<Key extends Comparable<? super Key>> implements Predicate<Key> {
        final private Key value;

        public GTPredicate(Key value) {
            this.value = value;
        }

        final public boolean isInterestedInKeysBetween(Key first, Key second) {
            return second==null || isInterestedInKey(second);
        }

        final public boolean isInterestedInKey(Key key) {
            return key.compareTo(value)>0;
        }

        @Override
        public String toString() {
            return "key > "+ value;
        }
    }

    class GTEPredicate<Key extends Comparable<? super Key>> implements Predicate<Key> {
        final private Key value;

        public GTEPredicate(Key value) {
            this.value = value;
        }

        final public boolean isInterestedInKeysBetween(Key first, Key second) {
            return second==null || isInterestedInKey(second);
        }

        final public boolean isInterestedInKey(Key key) {
            return key.compareTo(value)>=0;
        }

        @Override
        public String toString() {
            return "key >= "+ value;
        }
    }

    class LTPredicate<Key extends Comparable<? super Key>> implements Predicate<Key> {
        final private Key value;

        public LTPredicate(Key value) {
            this.value = value;
        }

        final public boolean isInterestedInKeysBetween(Key first, Key second) {
            return first==null || isInterestedInKey(first);
        }

        final public boolean isInterestedInKey(Key key) {
            return key.compareTo(value)<0;
        }

        @Override
        public String toString() {
            return "key < "+ value;
        }
    }

    class LTEPredicate<Key extends Comparable<? super Key>> implements Predicate<Key> {
        final private Key value;

        public LTEPredicate(Key value) {
            this.value = value;
        }

        final public boolean isInterestedInKeysBetween(Key first, Key second) {
            return first==null || isInterestedInKey(first);
        }

        final public boolean isInterestedInKey(Key key) {
            return key.compareTo(value)<=0;
        }

        @Override
        public String toString() {
            return "key <= "+ value;
        }
    }    
}