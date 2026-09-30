package io.github.green4j.sdsm;

import java.util.ArrayList;
import java.util.List;

/**
 * Where a cluster's sources are heard. A loop takes what its feeds are offered, keeps the
 * last word per id, and hands the lot to the structure: it writes nothing itself and waits
 * for nothing, so a source that has gone quiet, or a structure that is busy, holds up no
 * other loop.
 */
public final class EventLoop {

    private final LoopGroup group;
    private final LoopThread thread;
    private final List<Feed<?>> feeds = new ArrayList<>();

    EventLoop(final LoopGroup group, final LoopThread thread) {
        this.group = group;
        this.thread = thread;
    }

    /**
     * Puts a source on this loop and starts it.
     *
     * @param sourceId     the caller's own number for the source, above {@link Structure#NO_SOURCE},
     *                     carried by every property the materializer writes
     * @param source       where the observations come from
     * @param materializer what makes them into structure
     * @param <O>          what the source observes
     * @return the feed the source pushes into
     */
    public <O extends Observation> Feed<O> attach(final int sourceId,
                                                  final Source<O> source,
                                                  final Materializer<O> materializer) {
        if (sourceId <= Structure.NO_SOURCE) {
            throw new IllegalArgumentException("sourceId must be above NO_SOURCE: " + sourceId);
        }
        final Feed<O> feed;
        synchronized (feeds) {
            if (group.isClosed()) {
                throw new IllegalStateException("LoopGroup is closed");
            }
            feed = new Feed<>(this, group.structure(), sourceId, source, materializer);
            feeds.add(feed);
        }
        source.start(feed);
        return feed;
    }

    /**
     * Closes every feed on this loop. The thread belongs to the group, not to the loop.
     */
    public void close() {
        final Feed<?>[] snapshot;
        synchronized (feeds) {
            snapshot = feeds.toArray(new Feed<?>[0]);
        }
        for (int i = 0; i < snapshot.length; i++) {
            snapshot[i].close();
        }
    }

    void execute(final Runnable task) {
        thread.execute(task);
    }

    void ready(final Feed<?> feed) {
        thread.ready(feed);
    }

    void detach(final Feed<?> feed) {
        synchronized (feeds) {
            feeds.remove(feed);
        }
    }
}
