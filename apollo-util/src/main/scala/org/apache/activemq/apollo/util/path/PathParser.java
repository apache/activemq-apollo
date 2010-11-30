package org.apache.activemq.apollo.util.path;

import org.fusesource.hawtbuf.AsciiBuffer;
import org.fusesource.hawtbuf.ByteArrayOutputStream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import static org.apache.activemq.apollo.util.path.Part.*;

/**
 * Holds the delimiters used to parse paths.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class PathParser {

    public static final PathParser DEFAULT = new PathParser();

    public AsciiBuffer any_descendant_wildcard = new AsciiBuffer("**");
    public AsciiBuffer any_child_wildcard = new AsciiBuffer("*");
    public AsciiBuffer path_seperator = new AsciiBuffer(".");



    public Path parsePath(AsciiBuffer subject) {
    	ArrayList<Part> list = new ArrayList<Part>(10);
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
        return new Path(new ArrayList(list));
    }

    private Part parsePart(AsciiBuffer value) {
        if( value.equals(any_child_wildcard) ) {
            return ANY_CHILD;
        } else if( value.equals(any_descendant_wildcard) ) {
            return ANY_DESCENDANT;
        } else {
            return new LiteralPart(value);
        }
    }

    /**
     * Converts the path back to the string representation.
     * @return
     */
    public String toString(Path path) {
        StringBuffer buffer = new StringBuffer();
        boolean first=true;
        for(Part p : path.parts) {
            if( !first ) {
                buffer.append(path_seperator);
            }
            buffer.append(p.toString(this));
            first = false;
        }
        return buffer.toString();
    }


    public void write(Path path, ByteArrayOutputStream os) throws IOException {
        boolean first=true;
        for(Part p : path.parts) {
            if( !first ) {
                path_seperator.writeTo(os);
            }
            new AsciiBuffer(p.toString(this)).writeTo(os);
            first = false;
        }
    }

    static interface PartFilter {
        public boolean matches(LinkedList<Part> remaining);
    }

    class LitteralPathFilter implements PartFilter {

        private final PartFilter next;
        private final LiteralPart path;

        public LitteralPathFilter(PartFilter next, LiteralPart path) {
            this.next = next;

            this.path = path;
        }
        public boolean matches(LinkedList<Part> remaining) {
            if( !remaining.isEmpty() ) {
                Part p = remaining.removeFirst();
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
        public boolean matches(LinkedList<Part> remaining) {
            if( !remaining.isEmpty() ) {
                Part p = remaining.removeFirst();
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
        public boolean matches(LinkedList<Part> remaining) {
            if( !remaining.isEmpty() ) {
                remaining.clear();
                return true;
            } else {
                return false;
            }
        }
    }

    public PathFilter parseFilter(AsciiBuffer path) {
        ArrayList<Part> parts = new ArrayList<Part>(parsePath(path).parts);
        Collections.reverse(parts);
        PartFilter last = null;
        for( Part p: parts) {
            if( p.isLiteral() ) {
                last = new LitteralPathFilter(last, (LiteralPart)p);
            } else if( p == ANY_CHILD ) {
                last = new AnyChildPathFilter(last);
            } else if( p == ANY_DESCENDANT ) {
                last = new AnyDecendentPathFilter(last);
            }
        }
        final PartFilter filter = last;
        return new PathFilter() {
            public boolean matches(Path path) {
                return filter.matches(new LinkedList(path.parts));
            }
        };
    }

    static public boolean containsWildCards(Path path) {
        for(Part p: path.parts) {
            if( p==ANY_DESCENDANT || p==ANY_CHILD) {
                return true;
            }
        }
        return false;
    }

}
