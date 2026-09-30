package io.github.green4j.sdsm;

/**
 * Turns what a source saw into what the structure holds. It runs on the structure's thread
 * inside the transaction the whole batch is, and writes through {@link Emit}: the structure
 * itself is not in its reach, so a materializer cannot start a task, wait on one, or read the
 * graph around what it is writing.
 * <p>
 * It is called once for an observation whose version assembly has not materialized yet, and
 * not at all for one repeating a version it already has.
 *
 * @param <O> what it materializes
 */
public interface Materializer<O extends Observation> {

    void materialize(O observation, Emit emit);

    /**
     * Says the feed has changed state, so a binding can put that in the graph - what a source
     * being unavailable means for the things it was modelling is the binding's to decide.
     *
     * @param state  the state now
     * @param reason why the source is unavailable, null in every other state
     * @param emit   writes nothing is swept for: objects made here belong to no area
     */
    default void stateChanged(final FeedState state, final Throwable reason, final Emit emit) {
    }
}
