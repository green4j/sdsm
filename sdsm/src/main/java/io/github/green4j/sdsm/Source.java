package io.github.green4j.sdsm;

import java.util.Collections;
import java.util.Map;

/**
 * Where observations come from. The contract is push: a source is handed the feed to push
 * into and is never asked for anything, so a source can watch, poll, or simply be fed by a
 * host that already holds a cache of its own.
 *
 * @param <O> what this source observes
 */
public interface Source<O extends Observation> {

    /**
     * @param feed where to push what it sees, from whichever thread it sees it on
     */
    void start(Feed<O> feed);

    void stop();

    /**
     * What this source writes that is not worth writing at every level: a property named here
     * is written only while the thing it belongs to is wanted at that level or higher.
     * Everything not named is written whenever the source sends it. Asked once, when the
     * source is attached.
     *
     * @return property name to the level it is worth at
     */
    default Map<String, DetailLevel> suppliedProperties() {
        return Collections.emptyMap();
    }

    /**
     * Says how much of one of the things this source speaks about is wanted now, on the loop
     * thread. This is where the fetching stops: a source paying per call stops calling for
     * what nobody is looking at, and a source paying nothing may ignore it.
     *
     * @param externalId the id the thing carries in the world this source watches
     * @param level      what is wanted of it
     */
    default void detailLevelChanged(final CharSequence externalId, final DetailLevel level) {
    }

    /**
     * Says an observation could not be materialized, on the loop thread, before it is handed
     * back. What was written before the failure stays and belongs to the observation's area; the
     * version is not remembered, so saying it again is tried again. Every source says what
     * becomes of it: a failure is never silent.
     *
     * @param observation the observation, or null when what failed was not one: the materializer
     *                    saying a change of state, or a batch the structure would not take
     * @param reason      why
     */
    void rejected(O observation, Throwable reason);

    /**
     * Hands an observation back once assembly is done with it, on the loop thread.
     *
     * @param observation the observation offered earlier
     */
    default void release(final O observation) {
    }
}
