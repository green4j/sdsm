package io.github.green4j.sdsm;

/**
 * How the children of a node come to one value on the node. {@code MAX} and {@code MIN}
 * cannot take a child back out, so a change there recounts the node's children.
 */
public enum Fold {
    SUM,
    MAX,
    MIN
}
