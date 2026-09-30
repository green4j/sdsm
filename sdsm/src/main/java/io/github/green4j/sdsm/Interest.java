package io.github.green4j.sdsm;

/**
 * What one client wants: a level for everything it has not spoken about, and a level for
 * every object it has. It is replaced whole rather than amended, because a display that has
 * just changed knows what it is showing and does not know what it told anyone last time.
 * <p>
 * An object the client has not named is wanted at the base level, which is what a thing
 * arriving under a collapsed parent gets until the client speaks again.
 */
public final class Interest {

    private final DetailLevel base;
    private final LongObjectMap<DetailLevel> levels = new LongObjectMap<>();

    /**
     * @param base the level for everything not named
     * @return a new interest
     */
    public static Interest of(final DetailLevel base) {
        if (base == null) {
            throw new IllegalArgumentException("base is required");
        }
        return new Interest(base);
    }

    private Interest(final DetailLevel base) {
        this.base = base;
    }

    /**
     * @param objectId the object
     * @param level    what is wanted of it
     * @return this interest, so overrides read as one phrase
     */
    public Interest at(final long objectId, final DetailLevel level) {
        if (level == null) {
            throw new IllegalArgumentException("level is required");
        }
        levels.put(objectId, level);
        return this;
    }

    /**
     * @return the level for everything not named
     */
    public DetailLevel base() {
        return base;
    }

    /**
     * @param objectId the object
     * @return what this client wants of it
     */
    public DetailLevel levelOf(final long objectId) {
        final DetailLevel named = levels.get(objectId);
        return named != null ? named : base;
    }

    int ordinalOf(final long objectId) {
        return levelOf(objectId).ordinal();
    }

    int overrideCount() {
        return levels.size();
    }

    long overrideIdAt(final int index) {
        return levels.keyAt(index);
    }

    Interest copy() {
        final Interest copy = new Interest(base);
        for (int i = 0; i < levels.size(); i++) {
            copy.levels.put(levels.keyAt(i), levels.valueAt(i));
        }
        return copy;
    }
}
