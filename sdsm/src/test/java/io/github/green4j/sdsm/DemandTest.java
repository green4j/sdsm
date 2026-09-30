package io.github.green4j.sdsm;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Demand runs the other way: the display says what it is looking at, and that has to reach
 * the source, which is the only place where work can actually be saved.
 */
class DemandTest {

    private static final int AREA = 1;
    private static final double CPU = 1.0;

    private final StructureRuntime runtime = StructureRuntime.create(1, 1, "demand");
    private final Structure structure = runtime.newStructure();
    private final LoopGroup loops = LoopGroup.create(structure, 1, "demand");
    private final EventLoop loop = loops.newLoop();

    @AfterEach
    void tearDown() {
        loops.close();
        runtime.close();
    }

    private static final class Pod implements Observation {
        private final String externalId;
        private final String version;

        private Pod(final String externalId, final String version) {
            this.externalId = externalId;
            this.version = version;
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
            return AREA;
        }
    }

    /**
     * A source that pays for what it fetches: it looks at a pod only while somebody wants it,
     * and counts what looking cost.
     */
    private static final class Pods implements Source<Pod> {
        private final String[] ids;
        private final Map<String, DetailLevel> wanted = new ConcurrentHashMap<>();
        private Feed<Pod> feed;
        private volatile int observations;

        private Pods(final String... ids) {
            this.ids = ids;
        }

        @Override
        public Map<String, DetailLevel> suppliedProperties() {
            return Collections.singletonMap("cpu", DetailLevel.FINE);
        }

        @Override
        public void start(final Feed<Pod> started) {
            this.feed = started;
        }

        @Override
        public void stop() {
        }

        @Override
        public void detailLevelChanged(final CharSequence externalId, final DetailLevel level) {
            wanted.put(externalId.toString(), level);
        }

        private DetailLevel told(final String externalId) {
            return wanted.getOrDefault(externalId, DetailLevel.FINE);
        }

        private void poll(final int round) {
            look(round);
            feed.complete(AREA);
        }

        private void look(final int round) {
            for (final String id : ids) {
                if (wanted.getOrDefault(id, DetailLevel.FINE) == DetailLevel.OFF) {
                    continue;
                }
                observations++;
                feed.observed(new Pod(id, "v" + round));
            }
        }
    }

    private static final class Placement implements Materializer<Pod> {
        @Override
        public void materialize(final Pod pod, final Emit emit) {
            final long node = emit.node(pod.externalId, pod.externalId, "pod");
            emit.setBoolean(node, emit.keys().idOf("ready"), true);
            emit.setDouble(node, emit.keys().idOf("cpu"), CPU);
        }
    }

    /**
     * A display names what it shows in whatever order it lays it out: an interest costs what it
     * names, however the names come.
     */
    @Test
    void shouldNameThingsAtTheCostOfTheNames() {
        final int named = 50_000;
        final long started = System.nanoTime();
        Interest interest = Interest.of(DetailLevel.COARSE);
        for (int id = named; id > 0; id--) {
            interest = interest.at(id, DetailLevel.FINE);
        }
        interest = interest.at(7L, DetailLevel.OFF);
        final long elapsed = System.nanoTime() - started;

        assertEquals(DetailLevel.OFF, interest.levelOf(7L));
        assertEquals(DetailLevel.FINE, interest.levelOf(named));
        assertEquals(DetailLevel.COARSE, interest.levelOf(named + 1));
        assertTrue(elapsed < TimeUnit.MILLISECONDS.toNanos(50L), elapsed / 1_000_000 + " ms");
    }

    /**
     * The acceptance: collapsing a group is the display saying it is no longer looking, and
     * what looking costs stops being paid; expanding it pays again. What the source was told
     * to stop looking at stays where it was - not being looked at is not being gone, so the
     * sweep leaves it alone and the display has something to expand back to.
     */
    @Test
    void shouldLookAtLessWhenLessIsWanted() {
        final Pods source = new Pods("pod-1", "pod-2", "pod-3");
        final Feed<Pod> feed = loop.attach(1, source, new Placement());

        source.poll(1);
        awaitState(feed, FeedState.CONVERGED);
        assertEquals(3, source.observations);
        assertEquals(3, count("node[type=pod]"));

        collapse(source, DetailLevel.OFF);
        source.poll(2);
        Await.until(() -> source.observations == 5);
        assertEquals(3, count("node[type=pod]"));
        assertFalse(hasCpu("pod-2"));

        collapse(source, DetailLevel.COARSE);
        source.poll(3);
        Await.until(() -> source.observations == 8);
        assertEquals(3, count("node[type=pod]"));
    }

    /**
     * A round the source began while told not to look ends after it has been told to look
     * again: what it skipped, it skipped on the old word, and that is not gone.
     */
    @Test
    void shouldKeepWhatARoundSkippedOnTheOldWord() {
        final Pods source = new Pods("pod-1", "pod-2");
        final Feed<Pod> feed = loop.attach(1, source, new Placement());
        source.poll(1);
        awaitState(feed, FeedState.CONVERGED);
        collapse(source, DetailLevel.OFF);

        source.look(2);
        collapse(source, DetailLevel.FINE);
        feed.complete(AREA);
        feed.removed("pod-1");
        Await.until(() -> objectWithExternalId("pod-1") == null);

        assertEquals(1, count("node[type=pod]"));
    }

    /**
     * A value the source would no longer send is unknown from the moment nobody wants it, not
     * from the next observation: what is on the screen would otherwise be a value nobody is
     * paying for any more.
     */
    @Test
    void shouldForgetWhatNobodyIsPayingFor() {
        final Pods source = new Pods("pod-1");
        final Feed<Pod> feed = loop.attach(1, source, new Placement());

        source.poll(1);
        awaitState(feed, FeedState.CONVERGED);
        assertTrue(hasCpu("pod-1"));

        structure.setInterest("window", Interest.of(DetailLevel.COARSE)).join();
        assertFalse(hasCpu("pod-1"));

        source.poll(2);
        Await.until(() -> source.observations == 2);
        assertFalse(hasCpu("pod-1"));
    }

    /**
     * I4: a value below the level it is supplied at is unknown, and a predicate does not
     * match what is unknown - either way round.
     */
    @Test
    void shouldNotMatchAPredicateOnWhatIsUnknown() {
        final Pods source = new Pods("pod-1");
        final Feed<Pod> feed = loop.attach(1, source, new Placement());

        source.poll(1);
        awaitState(feed, FeedState.CONVERGED);
        assertEquals(1, count("node[cpu=1.0]"));
        assertEquals(0, count("node[cpu!=1.0]"));

        structure.setInterest("window", Interest.of(DetailLevel.COARSE)).join();
        assertEquals(0, count("node[cpu=1.0]"));
        assertEquals(0, count("node[cpu!=1.0]"));
    }

    /**
     * A thing that arrives after a client has spoken is wanted at that client's base, and the
     * source is told so, as it would have been had the thing been there when the client spoke.
     */
    @Test
    void shouldTellTheSourceWhatIsWantedOfAThingThatArrives() {
        final Pods source = new Pods("pod-1");
        final Feed<Pod> feed = loop.attach(1, source, new Placement());
        structure.setInterest("window", Interest.of(DetailLevel.OFF)).join();

        source.poll(1);
        awaitState(feed, FeedState.CONVERGED);
        Await.until(() -> source.told("pod-1") == DetailLevel.OFF);
        source.poll(2);
        feed.unavailable(new IllegalStateException("a word that goes after the round"));
        awaitState(feed, FeedState.STALE);

        assertEquals(1, source.observations);
        assertEquals(1, count("node[type=pod]"), "not looked at is not gone");
    }

    /**
     * Demand is what anyone wants, not what the last one to speak wants: a thing one window
     * has put away stays whole while another is showing it.
     */
    @Test
    void shouldTakeTheHighestLevelAnyoneAsksFor() {
        final Pods source = new Pods("pod-1");
        final Feed<Pod> feed = loop.attach(1, source, new Placement());

        source.poll(1);
        awaitState(feed, FeedState.CONVERGED);

        structure.setInterest("away", Interest.of(DetailLevel.COARSE)).join();
        structure.setInterest("showing", Interest.of(DetailLevel.FINE)).join();
        source.poll(2);
        Await.until(() -> hasCpu("pod-1"));

        structure.clearInterest("showing").join();
        assertFalse(hasCpu("pod-1"));
    }

    /**
     * A source a closed group refused is not there: it takes nothing back when demand falls.
     */
    @Test
    void shouldLeaveNoTraceOfASourceItRefused() {
        final int cpu = structure.propertyKeys().idOf("cpu");
        structure.run(1, () -> structure.setDouble(
                structure.createNode("pod-1", "pod", "pod-1").id(), cpu, CPU)).join();
        loops.close();

        assertThrows(IllegalStateException.class, () -> loop.attach(1, new Pods(), new Placement()));
        structure.setInterest("window", Interest.of(DetailLevel.COARSE)).join();

        assertTrue(hasCpu("pod-1"));
    }

    /**
     * A window changing what it has put away costs what it changed, not what the structure holds.
     */
    @Test
    void shouldPayForANewInterestWhatItChanges() {
        final int objects = 200_000;
        final int changes = 1_000;
        final long[] ids = structure.submit(() -> {
            final long[] made = new long[objects];
            for (int i = 0; i < objects; i++) {
                made[i] = structure.createNode("pod-" + i, "pod").id();
            }
            return made;
        }).join();
        structure.setInterest("window", Interest.of(DetailLevel.FINE)).join();

        final long started = System.nanoTime();
        for (int i = 0; i < changes; i++) {
            structure.setInterest("window", Interest.of(DetailLevel.FINE).at(ids[i], DetailLevel.OFF)).join();
        }
        final long elapsed = System.nanoTime() - started;

        assertEquals(DetailLevel.OFF, levelOf(ids[changes - 1]));
        assertEquals(DetailLevel.FINE, levelOf(ids[changes - 2]));
        assertTrue(elapsed < TimeUnit.MILLISECONDS.toNanos(500L), elapsed / 1_000_000 + " ms");
    }

    /**
     * What nobody pays for goes as the demand falls, and a group may go with it: the demand
     * still reaches everything else.
     */
    @Test
    void shouldReachEverythingWhileAGroupGoes() {
        structure.run(() -> structure.groupBy("byCpu", "load", "node[type=pod]", "cpu")).join();
        final Pods source = new Pods("pod-1", "pod-2", "pod-3");
        final Feed<Pod> feed = loop.attach(1, source, new Placement());
        source.poll(1);
        awaitState(feed, FeedState.CONVERGED);
        final long rack = structure.submit(() -> structure.createNode("rack", "rack").id()).join();

        structure.setInterest("window", Interest.of(DetailLevel.COARSE)).join();

        assertEquals(0, count("node[type=load]"));
        assertEquals(DetailLevel.COARSE, levelOf(rack));
    }

    private DetailLevel levelOf(final long objectId) {
        return structure.submit(() -> structure.lookupOrNull(objectId).requiredLevel()).join();
    }

    @Test
    void shouldRefuseAClientWithoutAName() {
        assertThrows(IllegalArgumentException.class, () -> structure.clearInterest(null));
        assertThrows(IllegalArgumentException.class, () -> structure.clearInterest(" "));
    }

    /**
     * What the display wants reaches the source on the source's own thread, so a round polled
     * before it got there would be a round of the old answer.
     *
     * @param source the source that has to hear it
     * @param level  what is wanted of pod-2 now
     */
    private void collapse(final Pods source, final DetailLevel level) {
        final long podTwo = objectWithExternalId("pod-2").id();
        structure.setInterest("window", Interest.of(DetailLevel.FINE).at(podTwo, level)).join();
        Await.until(() -> toldLevel("pod-2") == level && source.told("pod-2") == level);
    }

    private DetailLevel toldLevel(final String externalId) {
        return structure.submit(() -> {
            final StructureObject object = structure.findByExternalId(externalId);
            return object == null ? null : object.requiredLevel();
        }).join();
    }

    private boolean hasCpu(final String externalId) {
        final StructureObject object = objectWithExternalId(externalId);
        return structure.snapshotObject(object.id()).join().containsKey("cpu");
    }

    private int count(final String selectorText) {
        return structure.matchedObjectIds(selectorText).join().length;
    }

    private StructureObject objectWithExternalId(final String externalId) {
        return structure.submit(() -> structure.findByExternalId(externalId)).join();
    }

    private void awaitState(final Feed<Pod> feed, final FeedState expected) {
        Await.until(() -> feed.state() == expected);
    }
}
