package io.github.green4j.sdsm;

/**
 * One thing as a source saw it. What it saw is the source's own business: assembly reads
 * nothing out of an observation but the three answers below, and the materializer that reads
 * the rest is written against the same type.
 * <p>
 * An observation offered to a {@link Feed} belongs to the feed until the source is given it
 * back through {@link Source#release(Observation)}, so a source pooling observations must
 * wait for that and a source that does not pool need do nothing.
 */
public interface Observation {

    /**
     * @return the id the observed thing carries in the world that owns it; the same text for
     *         as long as the feed holds this observation
     */
    CharSequence externalId();

    /**
     * @return what that world calls this state of the thing, or null when it names none;
     *         assembly compares it with the version it last materialized and never reads
     *         anything out of it
     */
    CharSequence version();

    /**
     * @return the area this observation belongs to, in the source's own numbering; a source
     *         that sends the whole of an area says so with {@link Feed#complete(int)}, and
     *         everything else it had put there is then swept away
     */
    int scope();
}
