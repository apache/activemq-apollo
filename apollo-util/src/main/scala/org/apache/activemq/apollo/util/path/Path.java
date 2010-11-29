package org.apache.activemq.apollo.util.path;

/**
 * Holds the delimiters used to parse paths.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
abstract public class Path {

    public boolean matches(Path p) {
        return true;
    }

    abstract public String toString(PathParser parser);

    public boolean isLiteral() {
        return false;
    }

}
