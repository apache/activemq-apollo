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
package org.apache.activemq.filter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

/**
 * An implementation of {@link DestinationNode} which navigates all the children of the given node
 * ignoring the name of the current path (so for navigating using * in a wildcard).
 *
 * @version $Revision: 563921 $
 */
public class AnyChildDestinationNode<Value> implements DestinationNode<Value> {
    private DestinationNode<Value> node;

    public AnyChildDestinationNode(DestinationNode<Value> node) {
        this.node = node;
    }

    public void appendMatchingValues(Set<Value> answer, String[] paths, int startIndex) {
    	for (DestinationNode<Value> child : getChildNodes()) {
            child.appendMatchingValues(answer, paths, startIndex);
        }
    }


    public void appendMatchingWildcards(Set<Value> answer, String[] paths, int startIndex) {
    	for (DestinationNode<Value> child : getChildNodes()) {
            child.appendMatchingWildcards(answer, paths, startIndex);
        }
    }


    public void appendDescendantValues(Set<Value> answer) {
    	for (DestinationNode<Value> child : getChildNodes()) {
            child.appendDescendantValues(answer);
        }
    }

    public DestinationNode<Value> getChild(String path) {
        final Collection<DestinationNode<Value>> list = new ArrayList<DestinationNode<Value>>();
    	for (DestinationNode<Value> child : getChildNodes()) {
            DestinationNode<Value> answer = child.getChild(path);
            if (answer != null) {
                list.add(answer);
            }
        }
        if (!list.isEmpty()) {
            return new AnyChildDestinationNode<Value>(this) {
                protected Collection<DestinationNode<Value>> getChildNodes() {
                    return list;
                }
            };
        }
        return null;
    }

    public Collection<Value> getDesendentValues() {
        Collection<Value> answer = new ArrayList<Value>();
    	for (DestinationNode<Value> child : getChildNodes()) {
            answer.addAll(child.getDesendentValues());
        }
        return answer;
    }

    public Collection<Value> getValues() {
        Collection<Value> answer = new ArrayList<Value>();
    	for (DestinationNode<Value> child : getChildNodes()) {
            answer.addAll(child.getValues());
        }
        return answer;
    }


    public Collection<DestinationNode<Value>> getChildren() {
        Collection<DestinationNode<Value>>  answer = new ArrayList<DestinationNode<Value>> ();
    	for (DestinationNode<Value> child : getChildNodes()) {
            answer.addAll(child.getChildren());
        }
        return answer;
    }

    public Collection<Value> removeDesendentValues() {
        Collection<Value> answer = new ArrayList<Value>();
    	for (DestinationNode<Value> child : getChildNodes()) {
            answer.addAll(child.removeDesendentValues());
        }
        return answer;
    }

    public Collection<Value> removeValues() {
        Collection<Value> answer = new ArrayList<Value>();
    	for (DestinationNode<Value> child : getChildNodes()) {
            answer.addAll(child.removeValues());
        }
        return answer;
    }

    protected Collection<DestinationNode<Value>> getChildNodes() {
        return node.getChildren();
    }
}


