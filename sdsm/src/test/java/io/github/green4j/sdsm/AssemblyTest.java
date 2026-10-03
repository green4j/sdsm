package io.github.green4j.sdsm;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * A source says what it sees; the structure has to end up saying the same thing, and to go on
 * saying it when the source stops seeing. Nothing here waits on anything: the loop hands the
 * work to the structure, and the test waits for the structure to have done it.
 */
class AssemblyTest {

    private static final int AREA_A = 1;
    private static final int AREA_B = 2;

    private final StructureRuntime runtime = StructureRuntime.create(1, 1, "assembly");
    private final Structure structure = runtime.newStructure();
    private final LoopGroup loops = LoopGroup.create(structure, 2, "assembly");
    private final EventLoop loop = loops.newLoop();

    @AfterEach
    void tearDown() {
        loops.close();
        runtime.close();
    }

    private static final class Pod implements Observation {
        private final String externalId;
        private final String version;
        private final int scope;

        private Pod(final String externalId, final String version, final int scope) {
            this.externalId = externalId;
            this.version = version;
            this.scope = scope;
        }

        @Override
        public CharSequence externalId() {
            return externalId;
        }

        @Override
        public CharSequence version() {
            return version;
        }

        @Override
        public int scope() {
            return scope;
        }
    }

    /**
     * Pods into the placement axis: a region holding clusters holding pods. The cluster is
     * also where this binding decides the state of a source belongs - the framework has no
     * opinion about that.
     */
    private static final class Placement implements Materializer<Pod> {
        private final String region;
        private final String cluster;
        private volatile int materialized;

        private Placement(final String region, final String cluster) {
            this.region = region;
            this.cluster = cluster;
        }

        @Override
        public void materialize(final Pod pod, final Emit emit) {
            final long clusterGroup = clusterGroup(emit);
            final long node = emit.node(pod.externalId, pod.externalId, "pod");
            emit.contain(clusterGroup, node, "placement");
            emit.setBoolean(node, emit.keys().idOf("ready"), true);
            materialized++;
        }

        @Override
        public void stateChanged(final FeedState state, final Throwable reason,
                                 final Emit emit) {
            emit.setText(clusterGroup(emit), emit.keys().idOf("state"), state.name());
        }

        private long clusterGroup(final Emit emit) {
            final long regionGroup = emit.node("region:" + region, region, "region");
            final long group = emit.node("cluster:" + cluster, cluster, "cluster");
            emit.contain(regionGroup, group, "placement");
            return group;
        }
    }

    /**
     * A source owning no polling of its own: the test pushes into it, which is what a host
     * already holding a cache of observations does.
     */
    private static final class Pushed implements Source<Pod> {
        private Feed<Pod> feed;
        private volatile int released;
        private final List<String> rejected = new CopyOnWriteArrayList<>();

        @Override
        public void start(final Feed<Pod> started) {
            this.feed = started;
        }

        @Override
        public void stop() {
        }

        @Override
        public void release(final Pod observation) {
            released++;
        }

        @Override
        public void rejected(final Pod observation, final Throwable reason) {
            rejected.add(observation.externalId + ": " + reason.getMessage());
        }

        private void see(final String externalId, final String version, final int scope) {
            feed.observed(new Pod(externalId, version, scope));
        }
    }

    /**
     * Places a pod as {@link Placement} does, then fails on the pods named {@code bad-*}: what it
     * wrote before failing is in the structure, as a task that fails is not rolled back.
     */
    private static final class Fragile implements Materializer<Pod> {
        private final Placement placement = new Placement("eu-de", "eu-de-1");

        @Override
        public void materialize(final Pod pod, final Emit emit) {
            placement.materialize(pod, emit);
            if (pod.externalId.startsWith("bad-")) {
                throw new IllegalStateException("cannot read " + pod.externalId);
            }
        }
    }

    @Test
    void shouldMakeOneObjectPerObservedThingAndConvergeWhenTheAreaIsComplete() {
        final Pushed source = new Pushed();
        final Feed<Pod> feed = loop.attach(1, source, new Placement("eu-de", "eu-de-1"));

        source.see("pod-1", "v1", AREA_A);
        source.see("pod-2", "v1", AREA_A);
        feed.complete(AREA_A);

        awaitState(feed, FeedState.CONVERGED);
        assertEquals(2, count("node[type=pod]"));
        assertEquals(2, count("node[axis=placement]"));
    }

    /**
     * An observation repeating a version it has already been given is not materialized again,
     * and still says the thing is there: the sweep that follows leaves it alone.
     */
    @Test
    void shouldNotMaterializeAVersionItAlreadyHas() {
        final Pushed source = new Pushed();
        final Placement placement = new Placement("eu-de", "eu-de-1");
        final Feed<Pod> feed = loop.attach(1, source, placement);

        source.see("pod-1", "v1", AREA_A);
        feed.complete(AREA_A);
        awaitState(feed, FeedState.CONVERGED);

        source.see("pod-1", "v1", AREA_A);
        source.see("pod-2", "v1", AREA_A);
        feed.complete(AREA_A);
        Await.until(() -> objectWithExternalId("pod-2") != null);

        assertEquals(2, placement.materialized);
        assertNotNull(objectWithExternalId("pod-1"));
    }

    /**
     * Everything a source says while the loop is held up is one batch, and a batch carries at
     * most one observation of a thing: the last one.
     */
    @Test
    void shouldKeepOnlyTheLastOfSeveralObservationsOfOneThing() {
        final Pushed source = new Pushed();
        final Placement placement = new Placement("eu-de", "eu-de-1");
        final Feed<Pod> feed = loop.attach(1, source, placement);
        final CountDownLatch held = holdTheLoop();

        source.see("pod-1", "v1", AREA_A);
        source.see("pod-1", "v2", AREA_A);
        source.see("pod-1", "v3", AREA_A);
        feed.complete(AREA_A);
        held.countDown();

        awaitState(feed, FeedState.CONVERGED);
        assertEquals(1, placement.materialized);
        Await.until(() -> source.released == 3);
    }

    @Test
    void shouldSweepOnlyWhatTheAreaNoLongerHolds() {
        final Pushed source = new Pushed();
        final Feed<Pod> feed = loop.attach(1, source, new Placement("eu-de", "eu-de-1"));

        source.see("pod-1", "v1", AREA_A);
        source.see("pod-2", "v1", AREA_A);
        source.see("pod-3", "v1", AREA_A);
        feed.complete(AREA_A);
        awaitState(feed, FeedState.CONVERGED);

        source.see("pod-1", "v1", AREA_A);
        source.see("pod-3", "v1", AREA_A);
        feed.complete(AREA_A);
        Await.until(() -> count("node[type=pod]") == 2);

        assertNotNull(objectWithExternalId("pod-1"));
        assertNull(objectWithExternalId("pod-2"));
        assertNotNull(objectWithExternalId("pod-3"));
    }

    /**
     * A group is the grouping's, not a source's: an observation naming its id is refused, and
     * the sweep that follows cannot take the group away.
     */
    @Test
    void shouldRefuseAnObservationNamingAGroup() {
        final Pushed source = new Pushed();
        final Feed<Pod> feed = loop.attach(1, source, new Placement("eu-de", "eu-de-1"));
        source.see("pod-1", "v1", AREA_A);
        feed.complete(AREA_A);
        awaitState(feed, FeedState.CONVERGED);
        structure.submit(() -> structure.groupBy("ready", "band", "node[type=pod]", "ready")).join();

        source.see("pod-1", "v1", AREA_A);
        source.see("ready:true", "v1", AREA_A);
        feed.complete(AREA_A);
        Await.until(() -> source.rejected.size() == 1);
        awaitTheLoop();

        assertEquals(1, count("node[type=band]"));
    }

    /**
     * A source is a number of the caller's own, above {@link Structure#NO_SOURCE}: at it, a feed
     * would take what nobody wrote - a derived value, a write by hand - for its own to drop.
     */
    @Test
    void shouldRefuseASourceNumberedAsNoSource() {
        assertThrows(IllegalArgumentException.class,
                () -> loop.attach(Structure.NO_SOURCE, new Pushed(), new Placement("eu-de", "eu-de-1")));
        assertThrows(IllegalArgumentException.class,
                () -> loop.attach(-1, new Pushed(), new Placement("eu-de", "eu-de-1")));
    }

    /**
     * Completing an area costs what the area holds, not what the source has put everywhere.
     */
    @Test
    @Timeout(5)
    void shouldSweepAnAreaAtTheCostOfTheArea() {
        final Pushed source = new Pushed();
        loop.attach(1, source, new Placement("eu-de", "eu-de-1"));
        roundOverAreas(source, 1);

        roundOverAreas(source, 2);

        assertEquals(AREAS * PODS_PER_AREA + 2, count("node[type=pod]"), "and a marker per round");
    }

    private static final int AREAS = 2_000;
    private static final int PODS_PER_AREA = 50;

    private void roundOverAreas(final Pushed source, final int round) {
        for (int area = 0; area < AREAS; area++) {
            for (int i = 0; i < PODS_PER_AREA; i++) {
                source.see("pod-" + area + "-" + i, "v1", AREA_B + area);
            }
            source.feed.complete(AREA_B + area);
        }
        source.see("round-" + round, "v1", AREA_A);
        Await.until(() -> objectWithExternalId("round-" + round) != null);
    }

    @Test
    void shouldTakeAwayWhatASourceSaysIsGone() {
        final Pushed source = new Pushed();
        final Feed<Pod> feed = loop.attach(1, source, new Placement("eu-de", "eu-de-1"));

        source.see("pod-1", "v1", AREA_A);
        source.see("pod-2", "v1", AREA_A);
        feed.complete(AREA_A);
        awaitState(feed, FeedState.CONVERGED);

        feed.removed("pod-2");
        Await.until(() -> count("node[type=pod]") == 1);

        assertNotNull(objectWithExternalId("pod-1"));
    }

    /**
     * The phase criterion, first half: a source going blind leaves what it modelled where it
     * is, marked, however incomplete the next thing it says turns out to be.
     */
    @Test
    void shouldKeepWhatAStaleSourceModelledAndSayThatItIsStale() {
        final Pushed source = new Pushed();
        final Placement placement = new Placement("eu-de", "eu-de-1");
        final Feed<Pod> feed = loop.attach(1, source, placement);

        source.see("pod-1", "v1", AREA_A);
        source.see("pod-2", "v1", AREA_A);
        feed.complete(AREA_A);
        awaitState(feed, FeedState.CONVERGED);

        feed.unavailable(new IllegalStateException("datadog is down"));
        awaitState(feed, FeedState.STALE);

        source.see("pod-1", "v2", AREA_A);
        feed.complete(AREA_A);
        Await.until(() -> placement.materialized == 3);

        assertEquals(2, count("node[type=pod]"));
        assertEquals("STALE", stateOf("cluster:eu-de-1"));

        feed.available();
        awaitState(feed, FeedState.CONVERGED);
        assertEquals("CONVERGED", stateOf("cluster:eu-de-1"));
    }

    /**
     * The phase criterion, second half: one region going stale is nothing to the other, which
     * goes on converging and sweeping on its own loop.
     */
    @Test
    void shouldGoOnConvergingOnOneLoopWhileTheOtherIsStale() {
        final Pushed east = new Pushed();
        final Feed<Pod> eastFeed = loop.attach(1, east, new Placement("eu-de", "eu-de-1"));
        final Pushed west = new Pushed();
        final Feed<Pod> westFeed = loops.newLoop()
                .attach(2, west, new Placement("us-east", "us-east-1"));

        east.see("east-1", "v1", AREA_A);
        eastFeed.complete(AREA_A);
        west.see("west-1", "v1", AREA_B);
        west.see("west-2", "v1", AREA_B);
        westFeed.complete(AREA_B);
        awaitState(eastFeed, FeedState.CONVERGED);
        awaitState(westFeed, FeedState.CONVERGED);

        eastFeed.unavailable(new IllegalStateException("cluster unreachable"));
        awaitState(eastFeed, FeedState.STALE);

        west.see("west-1", "v2", AREA_B);
        westFeed.complete(AREA_B);
        Await.until(() -> objectWithExternalId("west-2") == null);

        assertNotNull(objectWithExternalId("east-1"));
        assertEquals(FeedState.CONVERGED, westFeed.state());
    }

    /**
     * Two clusters put themselves in the same region, so the region outlives the first of
     * them to go: an object is swept when nothing says it is there any more, not when the
     * first thing to have said it stops. What is said to be there is the structure's to count,
     * so it makes no difference whether the two feeds share a group of loops.
     *
     * @param inAnotherGroup whether the second feed runs in a group of its own
     */
    @ParameterizedTest(name = "in another group: {0}")
    @ValueSource(booleans = {false, true})
    void shouldKeepAnObjectTwoFeedsBothSayIsThere(final boolean inAnotherGroup) {
        final Pushed first = new Pushed();
        final Feed<Pod> firstFeed = loop.attach(1, first, new Placement("eu-de", "eu-de-1"));
        final LoopGroup group = inAnotherGroup ? LoopGroup.create(structure, 1, "others") : loops;
        try {
            final Pushed second = new Pushed();
            final Feed<Pod> secondFeed = group.newLoop()
                    .attach(2, second, new Placement("eu-de", "eu-de-2"));

            first.see("pod-1", "v1", AREA_A);
            firstFeed.complete(AREA_A);
            second.see("pod-2", "v1", AREA_B);
            secondFeed.complete(AREA_B);
            awaitState(firstFeed, FeedState.CONVERGED);
            awaitState(secondFeed, FeedState.CONVERGED);
            assertEquals(3, count("node[axis=placement]"));

            firstFeed.complete(AREA_A);
            Await.until(() -> objectWithExternalId("pod-1") == null);

            assertNull(objectWithExternalId("cluster:eu-de-1"));
            assertNotNull(objectWithExternalId("region:eu-de"));
            assertEquals(1, count("under(placement, /eu-de/eu-de-2)"));
        } finally {
            if (group != loops) {
                group.close();
            }
        }
    }

    /**
     * Feeds sharing a thread share its handover: what both said while the thread was held goes
     * over as one task, and each write still carries the source that made it.
     */
    @Test
    void shouldAttributeEachBatchOfOneHandoverToItsOwnSource() {
        final Pushed first = new Pushed();
        final Feed<Pod> firstFeed = loop.attach(1, first, new Placement("eu-de", "eu-de-1"));
        final Pushed second = new Pushed();
        final Feed<Pod> secondFeed = loop.attach(2, second, new Placement("eu-de", "eu-de-2"));
        final CountDownLatch held = holdTheLoop();

        first.see("pod-1", "v1", AREA_A);
        firstFeed.complete(AREA_A);
        second.see("pod-2", "v1", AREA_B);
        secondFeed.complete(AREA_B);
        held.countDown();

        awaitState(firstFeed, FeedState.CONVERGED);
        awaitState(secondFeed, FeedState.CONVERGED);
        final int ready = structure.propertyKeys().idOf("ready");
        assertEquals(1, objectWithExternalId("pod-1").sourceOf(ready));
        assertEquals(2, objectWithExternalId("pod-2").sourceOf(ready));
    }

    /**
     * A feed that gets something ready while its thread's handover is in flight goes over when
     * that handover comes back, even though nothing it carried has anything more to say.
     */
    @Test
    void shouldHandOverWhatGotReadyWhileTheThreadWasInFlight() {
        final Pushed first = new Pushed();
        final Feed<Pod> firstFeed = loop.attach(1, first, new Placement("eu-de", "eu-de-1"));
        final Pushed second = new Pushed();
        final Feed<Pod> secondFeed = loop.attach(2, second, new Placement("eu-de", "eu-de-2"));
        final CountDownLatch worker = holdTheStructure();

        first.see("pod-1", "v1", AREA_A);
        firstFeed.complete(AREA_A);
        awaitTheLoop();
        second.see("pod-2", "v1", AREA_B);
        secondFeed.complete(AREA_B);
        awaitTheLoop();
        worker.countDown();

        awaitState(firstFeed, FeedState.CONVERGED);
        awaitState(secondFeed, FeedState.CONVERGED);
    }

    /**
     * One observation that cannot be materialized is its own business: the rest of what the
     * source said goes in, the area is swept, the source is told, and what the failed one made
     * belongs to its area like anything else.
     */
    @Test
    void shouldGoOnWithTheRestOfABatchWhenOneObservationFails() {
        final Pushed source = new Pushed();
        final Feed<Pod> feed = loop.attach(1, source, new Fragile());
        final CountDownLatch held = holdTheLoop();

        source.see("pod-1", "v1", AREA_A);
        source.see("bad-1", "v1", AREA_A);
        source.see("pod-2", "v1", AREA_A);
        feed.complete(AREA_A);
        held.countDown();

        awaitState(feed, FeedState.CONVERGED);
        assertNotNull(objectWithExternalId("pod-1"));
        assertNotNull(objectWithExternalId("pod-2"));
        Await.until(() -> source.rejected.size() == 1);
        assertEquals(List.of("bad-1: cannot read bad-1"), source.rejected);
        Await.until(() -> source.released == 3);

        source.see("pod-1", "v1", AREA_A);
        source.see("pod-2", "v1", AREA_A);
        feed.complete(AREA_A);
        Await.until(() -> objectWithExternalId("bad-1") == null);
    }

    /**
     * A version that failed was not materialized, so saying it again is not a repeat.
     */
    @Test
    void shouldMaterializeAgainAVersionThatFailed() {
        final Pushed source = new Pushed();
        final Fragile fragile = new Fragile();
        final Feed<Pod> feed = loop.attach(1, source, fragile);

        source.see("bad-1", "v1", AREA_A);
        feed.complete(AREA_A);
        Await.until(() -> source.rejected.size() == 1);
        source.see("bad-1", "v1", AREA_A);
        feed.complete(AREA_A);

        Await.until(() -> source.rejected.size() == 2);
        assertEquals(2, fragile.placement.materialized);
    }

    /**
     * Taking an object away is a materializer letting go of it: what another feed still says is
     * there stays, and what nobody says is there goes at once.
     */
    @Test
    void shouldTakeAwayOnlyWhatNothingElseSaysIsThere() {
        final Pushed first = new Pushed();
        final Feed<Pod> firstFeed = loop.attach(1, first, new Placement("eu-de", "eu-de-1"));
        final Pushed second = new Pushed();
        final Feed<Pod> secondFeed = loops.newLoop().attach(2, second, (pod, emit) -> {
            final long region = emit.node("region:eu-de", "eu-de", "region");
            final long scratch = emit.node("scratch:" + pod.externalId, "scratch", "scratch");
            emit.remove(scratch);
            emit.remove(region);
        });

        first.see("pod-1", "v1", AREA_A);
        firstFeed.complete(AREA_A);
        awaitState(firstFeed, FeedState.CONVERGED);
        second.see("pod-2", "v1", AREA_B);
        secondFeed.complete(AREA_B);
        awaitState(secondFeed, FeedState.CONVERGED);

        assertNotNull(objectWithExternalId("region:eu-de"), "the first feed still says it");
        assertNull(objectWithExternalId("scratch:pod-2"), "nobody says it");
    }

    /**
     * An observation says where its node is now: a pod that went to another cluster is taken from
     * under the one it was in, and the cluster nothing holds any more is swept.
     */
    @Test
    void shouldMoveANodeWhereItsObservationNowPutsIt() {
        final Pushed source = new Pushed();
        final Feed<Pod> feed = loop.attach(1, source, (pod, emit) ->
                new Placement("eu-de", "eu-de-" + pod.version).materialize(pod, emit));

        source.see("pod-1", "1", AREA_A);
        feed.complete(AREA_A);
        awaitState(feed, FeedState.CONVERGED);
        source.see("pod-1", "2", AREA_A);
        feed.complete(AREA_A);
        Await.until(() -> objectWithExternalId("cluster:eu-de-1") == null);

        assertEquals(1, count("node[type=pod] & under(placement, /eu-de/eu-de-2)"));
        assertEquals(List.of(), source.rejected);
    }

    private CountDownLatch holdTheLoop() {
        final CountDownLatch held = new CountDownLatch(1);
        loop.execute(() -> {
            try {
                held.await();
            } catch (final InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        });
        return held;
    }

    private CountDownLatch holdTheStructure() {
        final CountDownLatch held = new CountDownLatch(1);
        structure.submit(() -> {
            try {
                held.await();
            } catch (final InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
            return null;
        });
        return held;
    }

    private void awaitTheLoop() {
        final CountDownLatch reached = new CountDownLatch(1);
        loop.execute(reached::countDown);
        Await.until(() -> reached.getCount() == 0);
    }

    private int count(final String selectorText) {
        return structure.matchedObjectIds(selectorText).join().length;
    }

    private StructureObject objectWithExternalId(final String externalId) {
        return structure.submit(() -> structure.findByExternalId(externalId)).join();
    }

    private String stateOf(final String groupExternalId) {
        final StructureObject group = objectWithExternalId(groupExternalId);
        if (group == null) {
            return null;
        }
        final Map<String, Object> properties = structure.snapshotObject(group.id()).join();
        return (String) properties.get("state");
    }

    private void awaitState(final Feed<Pod> feed, final FeedState expected) {
        Await.until(() -> feed.state() == expected);
    }
}
