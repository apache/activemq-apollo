package org.apache.activemq.apollo.util.path;

import org.fusesource.hawtbuf.AsciiBuffer;
import org.fusesource.hawtbuf.ByteArrayOutputStream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;

/**
 * Holds the delimiters used to parse paths.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class PathParser {

    public static final RootPath ROOT = new RootPath();
    public static final AnyDescendantPath ANY_DESCENDANT = new AnyDescendantPath();
    public static final AnyChildPath ANY_CHILD = new AnyChildPath();

    public AsciiBuffer any_descendant_wildcard = new AsciiBuffer("**");
    public AsciiBuffer any_child_wildcard = new AsciiBuffer("*");
    public AsciiBuffer path_seperator = new AsciiBuffer(".");

    private static class RootPath extends Path {
        public String toString(PathParser parser) {
            return "";
        }
        public boolean matches(Path p) {
            return p == ROOT;
        }
    }

    private static class AnyChildPath extends Path {
        public String toString(PathParser parser) {
            return parser.any_child_wildcard.toString();
        }
    }

    private static class AnyDescendantPath extends Path {
        public String toString(PathParser parser) {
            return parser.any_descendant_wildcard.toString();
        }
    }

    class LiteralPath extends Path {

        private final AsciiBuffer value;

        public LiteralPath(AsciiBuffer value) {
            this.value = value;
        }
        public boolean isLiteral() {
            return true;
        }

        public String toString(PathParser parser) {
            return value.toString();
        }

        public boolean matches(Path p) {
            if( p.isLiteral() ) {
                return ((LiteralPath)p).value.equals(value);
            }
            // we match any type of wildcard..
            return true;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            LiteralPath that = (LiteralPath) o;
            return value.equals(that.value);
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }
    }

    public Path[] parsePath(AsciiBuffer subject) {
    	ArrayList<Path> list = new ArrayList<Path>(10);
        int previous = 0;
        int lastIndex = subject.getLength() - 1;
        while (true) {
            int idx = subject.indexOf(path_seperator, previous);
            if (idx < 0) {
            	AsciiBuffer buffer = subject.slice(previous, lastIndex + 1).ascii();
                list.add(parsePart(buffer));
                break;
            }
        	AsciiBuffer buffer = subject.slice(previous, idx).ascii();
            list.add(parsePart(buffer));
            previous = idx + 1;
        }
        return list.toArray(new Path[list.size()]);
    }

    private Path parsePart(AsciiBuffer value) {
        if( value.equals(any_child_wildcard) ) {
            return ANY_CHILD;
        } else if( value.equals(any_descendant_wildcard) ) {
            return ANY_DESCENDANT;
        } else {
            return new LiteralPath(value);
        }
    }

    /**
     * Converts the paths back to the string representation.
     *
     * @param paths
     * @return
     */
    public String toString(Path[] paths) {
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < paths.length; i++) {
            if (i > 0) {
                buffer.append(path_seperator);
            }
            buffer.append(paths[i].toString(this));
        }
        return buffer.toString();
    }

    public void write(Path[] paths, ByteArrayOutputStream os) {
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < paths.length; i++) {
            if (i > 0) {
                buffer.append(path_seperator);
            }
            buffer.append(paths[i].toString(this));
        }
    }

    static interface PartFilter {
        public boolean matches(LinkedList<Path> remaining);
    }

    class LitteralPathFilter implements PartFilter {

        private final PartFilter next;
        private final LiteralPath path;

        public LitteralPathFilter(PartFilter next, LiteralPath path) {
            this.next = next;

            this.path = path;
        }
        public boolean matches(LinkedList<Path> remaining) {
            if( !remaining.isEmpty() ) {
                Path p = remaining.removeFirst();
                if( !path.matches(p) ) {
                    return false;
                }
                if( next!=null ) {
                    return next.matches(remaining);
                } else {
                    return remaining.isEmpty();
                }
            } else {
                return false;
            }
        }
    }

    static class AnyChildPathFilter implements PartFilter {
        private final PartFilter next;

        public AnyChildPathFilter(PartFilter next) {
            this.next = next;
        }
        public boolean matches(LinkedList<Path> remaining) {
            if( !remaining.isEmpty() ) {
                Path p = remaining.removeFirst();
                if( next!=null ) {
                    return next.matches(remaining);
                } else {
                    return remaining.isEmpty();
                }
            } else {
                return false;
            }
        }
    }

    static class AnyDecendentPathFilter implements PartFilter {
        private final PartFilter next;

        public AnyDecendentPathFilter(PartFilter next) {
            this.next = next;
        }
        public boolean matches(LinkedList<Path> remaining) {
            if( !remaining.isEmpty() ) {
                remaining.clear();
                return true;
            } else {
                return false;
            }
        }
    }

    public PathFilter parseFilter(AsciiBuffer path) {
        Path[] paths = parsePath(path);
        Collections.reverse(Arrays.asList(paths));
        PartFilter last = null;
        for( Path p: paths ) {
            if( p.isLiteral() ) {
                last = new LitteralPathFilter(last, (LiteralPath)p);
            } else if( p == ANY_CHILD ) {
                last = new AnyChildPathFilter(last);
            } else if( p == ANY_DESCENDANT ) {
                last = new AnyDecendentPathFilter(last);
            }
        }
        final PartFilter filter = last;
        return new PathFilter() {
            public boolean matches(Path[] path) {
                return filter.matches(new LinkedList(Arrays.asList(path)));
            }
        };
    }

    static public boolean containsWildCards(Path[] paths) {
        for(Path p:paths) {
            if( p==ANY_DESCENDANT || p==ANY_CHILD) {
                return true;
            }
        }
        return false;
    }

}
