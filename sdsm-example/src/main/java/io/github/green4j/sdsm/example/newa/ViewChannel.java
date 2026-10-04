package io.github.green4j.sdsm.example.newa;

import io.github.green4j.jelly.ByteArray;
import io.github.green4j.jelly.JsonGenerator;
import io.github.green4j.newa.websocket.ClientSession;
import io.github.green4j.newa.websocket.subscriptions.Channel;
import io.github.green4j.newa.websocket.subscriptions.EntitySubscriptions;
import io.github.green4j.sdsm.BatchSubscriber;
import io.github.green4j.sdsm.DeliveryPolicy;
import io.github.green4j.sdsm.Structure;
import io.github.green4j.sdsm.StructureBatch;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * A view per entity: a session subscribes to {@code topology} and is sent that view, a snapshot
 * and the deltas after it. This is the whole of the join between SDSM and a websocket server -
 * a view is already a named, long-lived thing with subscribers, and so is an entity here.
 * <p>
 * A delta published while a session's snapshot is being built may reach the session first; what
 * it said is in the snapshot. A client therefore applies a snapshot as the whole view and then
 * only the deltas whose {@code seq} is past the snapshot's.
 * <p>
 * A session that has fallen behind is skipped rather than disconnected
 * ({@code withSkipOnBackPressure}), and a skipped frame is not a loss: it marks the session
 * behind, and writability brings it back through {@code onClientSessionSubscribed}, which is a
 * snapshot again. A snapshot is built from current state, as a task on the structure's own
 * thread - never on the session's.
 */
public final class ViewChannel extends Channel<ViewChannel.ViewFeed> {

    private final Structure structure;
    private ViewFeed staged;

    /**
     * @param structure the structure whose views this channel serves
     */
    public ViewChannel(final Structure structure) {
        this.structure = structure;
    }

    /**
     * Makes a view and puts it on the air under its own name. It becomes findable only once the
     * structure is delivering to it, so nobody can subscribe to a name that has no view behind it
     * yet. Nothing here waits: this is called from a server's event loop.
     *
     * @param name     what a client asks for it by
     * @param selector which objects it holds
     * @param keys     which properties it delivers, null for all
     * @param interval how often at most it delivers
     * @return the subscriptions of that view; failed with {@link IllegalStateException} if the
     *         name is taken
     */
    public CompletableFuture<ViewFeed> publish(final String name,
                                               final String selector,
                                               final Set<String> keys,
                                               final Duration interval) {
        if (getEntitySubscriptions(name) != null) {
            return CompletableFuture.failedFuture(taken(name));
        }
        return structure.createView(name, selector, keys,
                        DeliveryPolicy.minInterval(interval))
                .thenCompose(view -> {
                    final ViewFeed feed = new ViewFeed(name, structure, view.id(),
                            selector, keys, interval);
                    return structure.subscribe(view.id(), feed).thenApply(ignored -> feed);
                })
                .thenCompose(feed -> {
                    if (register(feed)) {
                        return CompletableFuture.completedFuture(feed);
                    }
                    return structure.removeView(feed.viewId)
                            .thenCompose(ignored -> CompletableFuture.failedFuture(taken(name)));
                });
    }

    /**
     * Takes a view off the air and out of the structure. Its sessions are told, then
     * unsubscribed, and only then is the view removed - so what they are told is this, and not
     * the error the removal would otherwise send them.
     *
     * @param name the view
     * @return whether there was one
     */
    public CompletableFuture<Boolean> withdraw(final String name) {
        final ViewFeed told = getEntitySubscriptions(name);
        if (told != null) {
            final String withdrawn = ErrorFrame.of(name, "withdrawn");
            told.forEachSession(session -> session.send(withdrawn));
        }
        final ViewFeed feed = removeEntitySubscriptions(name);
        if (feed == null) {
            return CompletableFuture.completedFuture(Boolean.FALSE);
        }
        return structure.removeView(feed.viewId).thenApply(ignored -> Boolean.TRUE);
    }

    private synchronized boolean register(final ViewFeed feed) {
        staged = feed;
        try {
            return getOrCreateEntitySubscriptions(feed.entityId()) == feed;
        } finally {
            staged = null;
        }
    }

    // A client subscribes only to what is known, so the one feed ever made here is the one
    // register() stages.
    @Override
    protected ViewFeed newEntitySubscriptions(final String entityId) {
        return staged;
    }

    private static IllegalStateException taken(final String name) {
        return new IllegalStateException("A view named '" + name + "' is already published");
    }

    /**
     * One view's subscribers, and the subscriber the structure delivers to. Both sides of the
     * join meet here: SDSM pushes batches in, newa fans the rendered frame out.
     */
    public static final class ViewFeed extends EntitySubscriptions implements BatchSubscriber {

        private final Structure structure;
        private final BatchJson frames;
        private final long viewId;
        private final String selector;
        private final Set<String> keys;
        private final Duration interval;

        ViewFeed(final String entityId,
                 final Structure structure,
                 final long viewId,
                 final String selector,
                 final Set<String> keys,
                 final Duration interval) {
            super(entityId);
            this.structure = structure;
            this.frames = new BatchJson(entityId);
            this.viewId = viewId;
            this.selector = selector;
            this.keys = keys;
            this.interval = interval;
        }

        /**
         * What the view was made of, as it was asked for.
         *
         * @param out where to say it
         */
        public void describe(final JsonGenerator out) {
            out.startObject();
            out.objectMember("name");
            out.stringValue(entityId(), true);
            out.objectMember("selector");
            out.stringValue(selector, true);
            out.objectMember("keys");
            if (keys == null) {
                out.nullValue();
            } else {
                out.startArray();
                for (final String key : keys) {
                    out.stringValue(key, true);
                }
                out.endArray();
            }
            out.objectMember("intervalMs");
            out.numberValue(interval.toMillis());
            out.endObject();
        }

        /**
         * What the structure owes this view, rendered once and sent to everyone who is
         * listening. Nobody listening means nothing to render: the publication sequence does
         * not move, and a session subscribing at this very moment is given a snapshot instead.
         *
         * @param batch what the view owes
         */
        @Override
        public void onBatch(final StructureBatch batch) {
            if (isEmpty()) {
                return;
            }
            publishTextAndRelease(copyOf(frames.render(batch)));
        }

        @Override
        public void onError(final Throwable reason) {
            final String error = ErrorFrame.of(entityId(), reason);
            forEachSession(session -> session.send(error));
        }

        /**
         * A whole statement of the view, for one session, built on the structure's thread; the
         * session is told if it cannot be.
         *
         * @param session            who asked
         * @param publicationSequence where the fan-out had got to
         */
        @Override
        protected void onClientSessionSubscribed(final ClientSession session,
                                                 final long publicationSequence) {
            final BatchJson forOne = new BatchJson(entityId());
            structure.snapshotView(viewId, batch -> session.send(text(forOne.render(batch))))
                    .whenComplete((ignored, failure) -> {
                        if (failure != null) {
                            session.send(ErrorFrame.of(entityId(), failure));
                        }
                    });
        }

        private static ByteBuf copyOf(final ByteArray rendered) {
            return Unpooled.copiedBuffer(rendered.array(), rendered.start(), rendered.length());
        }

        private static String text(final ByteArray rendered) {
            return new String(rendered.array(), rendered.start(), rendered.length(),
                    StandardCharsets.UTF_8);
        }
    }
}
