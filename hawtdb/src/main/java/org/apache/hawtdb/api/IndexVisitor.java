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

import java.util.Arrays;
import java.util.List;

/**
 * Interface used to selectively visit the entries in a BTree.
 *
 * @param <Key>
 * @param <Value>
 */
public interface IndexVisitor<Key,Value> {

    /**
     * Do you want to visit the range of BTree entries between the first and and second key?
     *
     * @param first if null indicates the range of values before the second key.
     * @param second if null indicates the range of values after the first key.
     * @return true if you want to visit the values between the first and second key.
     */
    boolean isInterestedInKeysBetween(Key first, Key second);

    /**
     * The keys and values of an index node.
     *
     * @param keys
     * @param values
     */
    void visit(List<Key> keys, List<Value> values);

    /**
     * @return true if the visitor has quenched it's thirst for more results
     */
    boolean isSatiated();

    /**
     * Uses a predicates to select the keys that will be visited.
     * 
     * @param <Key>
     * @param <Value>
     */
    class PredicateVisitor<Key, Value> implements IndexVisitor<Key, Value> {
        
        public static final int UNLIMITED=-1;

        private final Predicate<Key> predicate;
        private int limit;
        
        public PredicateVisitor(Predicate<Key> predicate) {
            this(predicate, UNLIMITED);
        }
        
        public PredicateVisitor(Predicate<Key> predicate, int limit) {
            this.predicate = predicate;
            this.limit = limit;
        }

        final public void visit(List<Key> keys, List<Value> values) {
            for( int i=0; i < keys.size() && !isSatiated(); i++) {
                Key key = keys.get(i);
                if( predicate.isInterestedInKey(key) ) {
                    if(limit > 0 )
                        limit--;
                    matched(key, values.get(i));
                }
            }
        }

        @Override
        public boolean isInterestedInKeysBetween(Key first, Key second) {
            return predicate.isInterestedInKeysBetween(first, second);
        }

        public boolean isSatiated() {
            return limit==0;
        }
        
        /**
         * Subclasses should override.  This method will be called for each key,
         * value pair that matches the predicate.
         * 
         * @param key
         * @param value
         */
        protected void matched(Key key, Value value) {
        }
        
        // 
        // Helper static methods to help create predicate expressions.
        //
        
        public static <Key> Predicate<Key> or(Predicate<Key>... conditions) {
            return new Predicate.OrPredicate<Key>(Arrays.asList(conditions));
        }
        
        public static <Key> Predicate<Key> or(List<Predicate<Key>> conditions) {
            return new Predicate.OrPredicate<Key>(conditions);
        }
        
        public static <Key> Predicate<Key> and(Predicate<Key>... conditions) {
            return new Predicate.AndPredicate<Key>(Arrays.asList(conditions));
        }
        
        public static <Key> Predicate<Key> and(List<Predicate<Key>> conditions) {
            return new Predicate.AndPredicate<Key>(conditions);
        }        

        public static <Key extends Comparable<? super Key>> Predicate<Key> gt(Key key) {
            return new Predicate.GTPredicate<Key>(key);
        }        
        public static <Key extends Comparable<? super Key>> Predicate<Key> gte(Key key) {
            return new Predicate.GTEPredicate<Key>(key);
        }        

        public static <Key extends Comparable<? super Key>> Predicate<Key> lt(Key key) {
            return new Predicate.LTPredicate<Key>(key);
        }        
        public static <Key extends Comparable<? super Key>> Predicate<Key> lte(Key key) {
            return new Predicate.LTEPredicate<Key>(key);
        }
        
        public static <Key extends Comparable<? super Key>> Predicate<Key> lte(Key first, Key last) {
            return new Predicate.BetweenPredicate<Key>(first, last);
        }
        
    }


}