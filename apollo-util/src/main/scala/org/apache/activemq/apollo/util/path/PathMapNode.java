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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.fusesource.hawtbuf.AsciiBuffer;

/**
 * An implementation class used to implement {@link PathMap}
 * 
 * @version $Revision: 1.2 $
 */
public class PathMapNode<Value> implements PathNode<Value> {
    protected static final AsciiBuffer ANY_CHILD = PathMap.ANY_CHILD;
    protected static final AsciiBuffer ANY_DESCENDENT = PathMap.ANY_DESCENDENT;

    // we synchronize at the PathMap level
    private PathMapNode<Value> parent;
    private List<Value> values = new ArrayList<Value>();
    private Map<AsciiBuffer, PathNode<Value>> childNodes = new HashMap<AsciiBuffer, PathNode<Value>>();
    private AsciiBuffer path = new AsciiBuffer("Root");
    private int pathLength;

    public PathMapNode(PathMapNode<Value> parent) {
        this.parent = parent;
        if (parent == null) {
            pathLength = 0;
        } else {
            pathLength = parent.pathLength + 1;
        }
    }

    /**
     * Returns the child node for the given named path or null if it does not
     * exist
     */
    public PathMapNode<Value> getChild(AsciiBuffer path) {
        return (PathMapNode<Value>)childNodes.get(path);
    }

    /**
     * Returns the child nodes
     */
    public Collection<PathNode<Value>> getChildren() {
        return childNodes.values();
    }

    public int getChildCount() {
        return childNodes.size();
    }

    /**
     * Returns the child node for the given named path, lazily creating one if
     * it does not yet exist
     */
    public PathMapNode<Value> getChildOrCreate(AsciiBuffer asciiBuffer) {
        PathMapNode<Value> answer = (PathMapNode<Value>)childNodes.get(asciiBuffer);
        if (answer == null) {
            answer = createChildNode();
            answer.path = asciiBuffer;
            childNodes.put(asciiBuffer, answer);
        }
        return answer;
    }

    /**
     * Returns a mutable List of the values available at this node in the tree
     */
    public List<Value> getValues() {
        return values;
    }

    /**
     * Returns a mutable List of the values available at this node in the tree
     */
    public List<Value> removeValues() {
        ArrayList<Value> v = new ArrayList<Value>(values);
        // parent.getAnyChildNode().getValues().removeAll(v);
        values.clear();
        pruneIfEmpty();
        return v;
    }

    public Set<Value> removeDesendentValues() {
        Set<Value> answer = new HashSet<Value>();
        removeDesendentValues(answer);
        return answer;
    }

    protected void removeDesendentValues(Set<Value> answer) {
        // if (anyChild != null) {
        // anyChild.removeDesendentValues(answer);
        // }
        answer.addAll(removeValues());
    }

    /**
     * Returns a list of all the values from this node down the tree
     */
    public Set<Value> getDesendentValues() {
        Set<Value> answer = new HashSet<Value>();
        appendDescendantValues(answer);
        return answer;
    }

    public void add(ArrayList<AsciiBuffer> paths, int idx, Value value) {
        if (idx >= paths.size()) {
            values.add(value);
        } else {
            // if (idx == paths.size() - 1) {
            // getAnyChildNode().getValues().add(value);
            // }
            // else {
            // getAnyChildNode().add(paths, idx + 1, value);
            // }
            getChildOrCreate(paths.get(idx)).add(paths, idx + 1, value);
        }
    }

    public void remove(ArrayList<AsciiBuffer> paths, int idx, Value value) {
        if (idx >= paths.size()) {
            values.remove(value);
            pruneIfEmpty();
        } else {
            // if (idx == paths.size() - 1) {
            // getAnyChildNode().getValues().remove(value);
            // }
            // else {
            // getAnyChildNode().remove(paths, idx + 1, value);
            // }
            getChildOrCreate(paths.get(idx)).remove(paths, ++idx, value);
        }
    }

    public void removeAll(Set<Value> answer, ArrayList<AsciiBuffer> paths, int startIndex) {
        PathNode<Value> node = this;
        int size = paths.size();
        for (int i = startIndex; i < size && node != null; i++) {

            AsciiBuffer path = paths.get(i);
            if (path.equals(ANY_DESCENDENT)) {
                answer.addAll(node.removeDesendentValues());
                break;
            }

            node.appendMatchingWildcards(answer, paths, i);
            if (path.equals(ANY_CHILD)) {
                // node = node.getAnyChildNode();
                node = new AnyChildPathNode<Value>(node);
            } else {
                node = node.getChild(path);
            }
        }

        if (node != null) {
            answer.addAll(node.removeValues());
        }

    }

    public void appendDescendantValues(Set<Value> answer) {
        answer.addAll(values);

        // lets add all the children too
        for (PathNode<Value> child : childNodes.values()) {
			child.appendDescendantValues(answer);
        }

        // TODO???
        // if (anyChild != null) {
        // anyChild.appendDescendantValues(answer);
        // }
    }

    /**
     * Factory method to create a child node
     */
    protected PathMapNode<Value> createChildNode() {
        return new PathMapNode<Value>(this);
    }

    /**
     * Matches any entries in the map containing wildcards
     */
    public void appendMatchingWildcards(Set<Value> answer, ArrayList<AsciiBuffer> paths, int idx) {
        if (idx - 1 > pathLength) {
            return;
        }
        PathMapNode<Value> wildCardNode = getChild(ANY_CHILD);
        if (wildCardNode != null) {
            wildCardNode.appendMatchingValues(answer, paths, idx + 1);
        }
        wildCardNode = getChild(ANY_DESCENDENT);
        if (wildCardNode != null) {
            answer.addAll(wildCardNode.getDesendentValues());
        }
    }

    public void appendMatchingValues(Set<Value> answer, ArrayList<AsciiBuffer> paths, int startIndex) {
        PathNode<Value> node = this;
        boolean couldMatchAny = true;
        int size = paths.size();
        for (int i = startIndex; i < size && node != null; i++) {
            AsciiBuffer path = paths.get(i);
            if (path.equals(ANY_DESCENDENT)) {
                answer.addAll(node.getDesendentValues());
                couldMatchAny = false;
                break;
            }

            node.appendMatchingWildcards(answer, paths, i);

            if (path.equals(ANY_CHILD)) {
                node = new AnyChildPathNode<Value>(node);
            } else {
                node = node.getChild(path);
            }
        }
        if (node != null) {
            answer.addAll(node.getValues());
            if (couldMatchAny) {
                // lets allow FOO.BAR to match the FOO.BAR.> entry in the map
                PathNode<Value> child = node.getChild(ANY_DESCENDENT);
                if (child != null) {
                    answer.addAll(child.getValues());
                }
            }
        }
    }

    public AsciiBuffer getPath() {
        return path;
    }

    protected void pruneIfEmpty() {
        if (parent != null && childNodes.isEmpty() && values.isEmpty()) {
            parent.removeChild(this);
        }
    }

    protected void removeChild(PathMapNode<Value> node) {
        childNodes.remove(node.getPath());
        pruneIfEmpty();
    }
}
