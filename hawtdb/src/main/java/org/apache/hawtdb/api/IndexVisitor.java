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
 * Interface used to selectively visit the entries in a sorted index.
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public interface IndexVisitor<Key, Value> {

    /**
     * Do you want to visit the range of BTree entries between the first and and
     * second key?
     * 
     * @param first
     *            if null indicates the range of values before the second key.
     * @param second
     *            if null indicates the range of values after the first key.
     * @return true if you want to visit the values between the first and second
     *         key.
     */
    boolean isInterestedInKeysBetween(Key first, Key second);

    /**
     * The keys and values of a BTree leaf node.
     * 
     * @param keys
     * @param values
     */
    void visit(List<Key> keys, List<Value> values);

    /**
     * If the visitor wishes to
     * 
     * @return
     */
    boolean isSatiated();

    abstract class GTVisitor<Key extends Comparable<? super Key>, Value> implements IndexVisitor<Key, Value> {
        final private Key value;
        int matches = Integer.MAX_VALUE;
        boolean limited;

        public GTVisitor(Key value) {
            this.value = value;
        }

        public GTVisitor(Key value, int limit) {
            this.value = value;
            limited = true;
            matches = limit;
        }

        public boolean isInterestedInKeysBetween(Key first, Key second) {
            return second == null || second.compareTo(value) > 0;
        }

        public void visit(List<Key> keys, List<Value> values) {
            for (int i = 0; i < keys.size() && !isSatiated(); i++) {
                Key key = keys.get(i);
                if (key.compareTo(value) > 0) {
                    matched(key, values.get(i));
                    if (limited) matches--;
                }
            }
        }

        public boolean isSatiated() {
            return limited && matches <= 0;
        }

        abstract protected void matched(Key key, Value value);
    }

    abstract class GTEVisitor<Key extends Comparable<? super Key>, Value> implements IndexVisitor<Key, Value> {
        final private Key value;
        int matches = Integer.MAX_VALUE;
        boolean limited;

        public GTEVisitor(Key value) {
            this.value = value;
        }
        
        public GTEVisitor(Key value, int limit) {
            this.value = value;
            limited = true;
            matches = limit;
        }

        public boolean isInterestedInKeysBetween(Key first, Key second) {
            return second == null || second.compareTo(value) >= 0;
        }

        public void visit(List<Key> keys, List<Value> values) {
            for (int i = 0; i < keys.size() && !isSatiated(); i++) {
                Key key = keys.get(i);
                if (key.compareTo(value) >= 0) {
                    matched(key, values.get(i));
                    if (limited) matches--;
                }
            }
        }

        public boolean isSatiated() {
            return limited && matches <= 0;
        }

        abstract protected void matched(Key key, Value value);
    }

    abstract class LTVisitor<Key extends Comparable<? super Key>, Value> implements IndexVisitor<Key, Value> {
        final private Key value;
        int matches = Integer.MAX_VALUE;
        boolean limited;

        public LTVisitor(Key value) {
            this.value = value;
        }
        
        public LTVisitor(Key value, int limit) {
            this.value = value;
            limited = true;
            matches = limit;
        }

        public boolean isInterestedInKeysBetween(Key first, Key second) {
            return first == null || first.compareTo(value) < 0;
        }

        public void visit(List<Key> keys, List<Value> values) {
            for (int i = 0; i < keys.size() && !isSatiated(); i++) {
                Key key = keys.get(i);
                if (key.compareTo(value) < 0) {
                    matched(key, values.get(i));
                    if (limited) matches--;
                }
            }
        }

        public boolean isSatiated() {
            return limited && matches <= 0;
        }

        abstract protected void matched(Key key, Value value);
    }

    abstract class LTEVisitor<Key extends Comparable<? super Key>, Value> implements IndexVisitor<Key, Value> {
        final private Key value;
        int matches = Integer.MAX_VALUE;
        boolean limited;

        public LTEVisitor(Key value) {
            this.value = value;
        }

        public LTEVisitor(Key value, int limit) {
            this.value = value;
            limited = true;
            matches = limit;
        }
        
        public boolean isInterestedInKeysBetween(Key first, Key second) {
            return first == null || first.compareTo(value) <= 0;
        }

        public void visit(List<Key> keys, List<Value> values) {
            for (int i = 0; i < keys.size() && !isSatiated(); i++) {
                Key key = keys.get(i);
                if (key.compareTo(value) <= 0) {
                    matched(key, values.get(i));
                    if (limited) matches--;
                }
            }
        }

        public boolean isSatiated() {
            return limited && matches <= 0;
        }

        abstract protected void matched(Key key, Value value);
    }
}