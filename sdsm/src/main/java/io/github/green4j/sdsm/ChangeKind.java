package io.github.green4j.sdsm;

/**
 * What happened to an object, as far as a view is concerned. A link changing an endpoint
 * arrives as {@link #PROPERTY_CHANGED} - the endpoints are intrinsic properties of the link.
 * <p>
 * Containment is the one relation a node can have several of - a parent per axis - so it has
 * records of its own: they name the child, and the parent is read off the record with
 * {@link ChangeCursor#parentId()}.
 */
public enum ChangeKind {
    ADDED,
    REMOVED,
    CONTAINED,
    UNCONTAINED,
    PROPERTY_CHANGED
}
