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

import org.fusesource.hawtbuf.AsciiBuffer;

/**
 * Holds the delimiters used to parse paths.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
abstract public class Part {

    public boolean matches(Part p) {
        return true;
    }

    abstract public String toString(PathParser parser);

    public boolean isLiteral() {
        return false;
    }

    public static final RootPart ROOT = new RootPart();
    public static final AnyDescendantPart ANY_DESCENDANT = new AnyDescendantPart();
    public static final AnyChildPart ANY_CHILD = new AnyChildPart();

    private static class RootPart extends Part {
        public String toString(PathParser parser) {
            return "";
        }
        public boolean matches(Part p) {
            return p == ROOT;
        }
    }

    private static class AnyChildPart extends Part {
        public String toString(PathParser parser) {
            return parser.any_child_wildcard.toString();
        }
    }

    private static class AnyDescendantPart extends Part {
        public String toString(PathParser parser) {
            return parser.any_descendant_wildcard.toString();
        }
    }

    static class LiteralPart extends Part {

        private final AsciiBuffer value;

        public LiteralPart(AsciiBuffer value) {
            this.value = value;
        }
        public boolean isLiteral() {
            return true;
        }

        public String toString(PathParser parser) {
            return value.toString();
        }

        public boolean matches(Part p) {
            if( p.isLiteral() ) {
                return ((LiteralPart)p).value.equals(value);
            }
            // we match any type of wildcard..
            return true;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            LiteralPart that = (LiteralPart) o;
            return value.equals(that.value);
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }
    }

}
