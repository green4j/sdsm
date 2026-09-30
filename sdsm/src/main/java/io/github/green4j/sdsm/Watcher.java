package io.github.green4j.sdsm;

/**
 * What watches the structure without being part of it - a view, a grouping - and what it reads
 * past the object it holds, so a change of one object reaches every watcher it can change.
 */
abstract class Watcher {

    /** A link reads the nodes at its ends. */
    static final int ENDS = 1;
    /** A link reads its ports. */
    static final int PORTS = 1 << 1;
    /** A port reads the node it is on. */
    static final int OWNER = 1 << 2;
    /** A node reads its parent on an axis. */
    static final int PARENT = 1 << 3;
    /** An object reads its ancestors on an axis. */
    static final int PLACEMENT = 1 << 4;

    /**
     * @param keyId a property key
     * @return whether a change of that property, wherever it is read from, can change it
     */
    abstract boolean reads(int keyId);

    /**
     * @return what it reads past the object it holds, as the flags above
     */
    abstract int reach();

    /**
     * @param selector what a watcher holds
     * @return what the selector reads past the object it tests
     */
    static int reachOf(final Selector.Expression selector) {
        return (selector.readsEnds() ? ENDS : 0) | (selector.readsPlacement() ? PLACEMENT : 0);
    }

    final boolean reaches(final int flag) {
        return (reach() & flag) != 0;
    }
}
