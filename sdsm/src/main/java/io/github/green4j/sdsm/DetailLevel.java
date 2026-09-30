package io.github.green4j.sdsm;

/**
 * How much of a thing is wanted. Demand runs the other way from everything else here: the
 * display says what it is looking at, and a source that pays for what it fetches stops paying
 * for what nobody is looking at.
 */
public enum DetailLevel {

    /**
     * Not wanted at all.
     */
    OFF,

    /**
     * Wanted as a thing on the screen: what it is, where it sits, whether it is well.
     */
    COARSE,

    /**
     * Wanted with everything known about it.
     */
    FINE;

    private static final DetailLevel[] ALL = values();

    static DetailLevel of(final int ordinal) {
        return ALL[ordinal];
    }
}
