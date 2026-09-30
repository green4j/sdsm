package io.github.green4j.sdsm;

/**
 * What a property value holds. A key that is not set reads as {@link #ABSENT}, and so does a
 * key a change record reports as removed.
 */
public enum ValueType {
    ABSENT,
    LONG,
    DOUBLE,
    BOOLEAN,
    TEXT
}
