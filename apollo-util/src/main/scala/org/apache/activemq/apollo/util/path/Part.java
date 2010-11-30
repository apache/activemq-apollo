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
