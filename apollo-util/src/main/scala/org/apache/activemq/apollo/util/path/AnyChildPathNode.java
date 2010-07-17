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
package org.apache.activemq.apollo.util.path;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

import org.fusesource.hawtbuf.AsciiBuffer;

/**
 * An implementation of {@link PathNode} which navigates all the children of the given node
 * ignoring the name of the current path (so for navigating using * in a wildcard).
 *
 */
public class AnyChildPathNode<Value> implements PathNode<Value> {
    private PathNode<Value> node;

    public AnyChildPathNode(PathNode<Value> node) {
        this.node = node;
    }

    public void appendMatchingValues(Set<Value> answer, ArrayList<AsciiBuffer> paths, int startIndex) {
    	for (PathNode<Value> child : getChildNodes()) {
            child.appendMatchingValues(answer, paths, startIndex);
        }
    }


    public void appendMatchingWildcards(Set<Value> answer, ArrayList<AsciiBuffer> paths, int startIndex) {
    	for (PathNode<Value> child : getChildNodes()) {
            child.appendMatchingWildcards(answer, paths, startIndex);
        }
    }


    public void appendDescendantValues(Set<Value> answer) {
    	for (PathNode<Value> child : getChildNodes()) {
            child.appendDescendantValues(answer);
        }
    }

    public PathNode<Value> getChild(AsciiBuffer path) {
        final Collection<PathNode<Value>> list = new ArrayList<PathNode<Value>>();
    	for (PathNode<Value> child : getChildNodes()) {
            PathNode<Value> answer = child.getChild(path);
            if (answer != null) {
                list.add(answer);
            }
        }
        if (!list.isEmpty()) {
            return new AnyChildPathNode<Value>(this) {
                protected Collection<PathNode<Value>> getChildNodes() {
                    return list;
                }
            };
        }
        return null;
    }

    public Collection<Value> getDesendentValues() {
        Collection<Value> answer = new ArrayList<Value>();
    	for (PathNode<Value> child : getChildNodes()) {
            answer.addAll(child.getDesendentValues());
        }
        return answer;
    }

    public Collection<Value> getValues() {
        Collection<Value> answer = new ArrayList<Value>();
    	for (PathNode<Value> child : getChildNodes()) {
            answer.addAll(child.getValues());
        }
        return answer;
    }


    public Collection<PathNode<Value>> getChildren() {
        Collection<PathNode<Value>>  answer = new ArrayList<PathNode<Value>> ();
    	for (PathNode<Value> child : getChildNodes()) {
            answer.addAll(child.getChildren());
        }
        return answer;
    }

    public Collection<Value> removeDesendentValues() {
        Collection<Value> answer = new ArrayList<Value>();
    	for (PathNode<Value> child : getChildNodes()) {
            answer.addAll(child.removeDesendentValues());
        }
        return answer;
    }

    public Collection<Value> removeValues() {
        Collection<Value> answer = new ArrayList<Value>();
    	for (PathNode<Value> child : getChildNodes()) {
            answer.addAll(child.removeValues());
        }
        return answer;
    }

    protected Collection<PathNode<Value>> getChildNodes() {
        return node.getChildren();
    }
}


