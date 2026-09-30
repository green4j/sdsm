package io.github.green4j.sdsm;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A fold over an object's own properties - what several sources each say about one thing, as
 * one value on it: the worst health any of them reports - and the worst of that over a parent.
 */
class OwnKeysTest {

    private final StructureRuntime runtime = StructureRuntime.create(1, 1, "own-keys");
    private final Structure structure = runtime.newStructure();
    private final int health = structure.propertyKeys().idOf("health");
    private final int k8s = structure.propertyKeys().idOf("health.k8s");
    private final int metrics = structure.propertyKeys().idOf("health.metrics");

    private Node release;
    private Node aggregator;
    private Node stream;

    @BeforeEach
    void layOut() {
        release = structure.submit(() -> structure.createNode("blue", "release")).join();
        aggregator = structure.submit(() -> structure.createNode("aggregator", "service")).join();
        stream = structure.submit(() -> structure.createNode("stream", "service")).join();
        structure.run(() -> structure.contain(release.id(), aggregator.id(), "placement")).join();
        structure.run(() -> structure.contain(release.id(), stream.id(), "placement")).join();
        structure.run(() -> {
            structure.derive(aggregator.id(), health, Fold.MAX, Over.keys(k8s, metrics));
            structure.derive(stream.id(), health, Fold.MAX, Over.keys(k8s, metrics));
            structure.derive(release.id(), health, Fold.MAX, Over.children(health, "service"));
        }).join();
    }

    @AfterEach
    void tearDown() {
        runtime.close();
    }

    @Test
    void shouldTakeTheWorstOfWhatIsSaidAndCarryItUp() {
        assertFalse(structure.snapshotObject(aggregator.id()).join().containsKey("health"));

        set(aggregator, k8s, 0L);
        set(aggregator, metrics, 2L);
        set(stream, k8s, 1L);
        assertEquals(2L, structure.snapshotObject(aggregator.id()).join().get("health"));
        assertEquals(2L, structure.snapshotObject(release.id()).join().get("health"));

        structure.run(() -> structure.removeProperty(aggregator.id(), metrics)).join();
        assertEquals(0L, structure.snapshotObject(aggregator.id()).join().get("health"));
        assertEquals(1L, structure.snapshotObject(release.id()).join().get("health"));
    }

    /**
     * On one object a key is either what a fold reads or what it writes: a derived key is not
     * folded into another, and a key folded into another is not derived.
     */
    @Test
    void shouldRefuseToFoldWhatIsDerivedOrDeriveWhatIsFolded() {
        final int worst = structure.propertyKeys().idOf("worst");
        assertRejected(() -> structure.run(() -> structure.derive(
                aggregator.id(), worst, Fold.MAX, Over.keys(health))).join());
        assertRejected(() -> structure.run(() -> structure.derive(
                aggregator.id(), k8s, Fold.MAX, Over.keys(metrics))).join());
    }

    /**
     * An intrinsic property changes with the shape of the graph, not by a write, so no fold
     * could follow it; and a key id is never negative.
     */
    @Test
    void shouldRefuseToFoldWhatIsNotAKeyOrIsIntrinsic() {
        assertThrows(IllegalArgumentException.class, () -> Over.keys(k8s, -1));
        assertThrows(IllegalArgumentException.class, () -> Over.keys(PropertyKeys.LINKS));
        assertThrows(IllegalArgumentException.class, () -> Over.children(PropertyKeys.AXIS, null));
        assertThrows(IllegalArgumentException.class,
                () -> Over.keys(k8s).andChildren(PropertyKeys.NAME, null));
    }

    @Test
    void shouldFoldWhatIsSaidOfAComponentAndOfWhatItRunsOnAsOne() {
        final Node query = structure.submit(() -> structure.createNode("todayquery", "service")).join();
        final Node pod = structure.submit(() -> structure.createNode("todayquery-0", "pod")).join();
        structure.run(() -> structure.contain(release.id(), query.id(), "placement")).join();
        structure.run(() -> structure.contain(query.id(), pod.id(), "deployment")).join();
        structure.run(() -> structure.derive(query.id(), health, Fold.MAX,
                Over.keys(k8s, metrics).andChildren(health, "pod"))).join();

        set(query, k8s, 0L);
        assertEquals(0L, value(query, "health"));
        assertEquals(1L, value(query, "health.known"));
        assertEquals(3L, value(query, "health.total"));

        set(pod, health, 2L);   // what it runs on
        assertEquals(2L, value(query, "health"));
        assertEquals(2L, value(release, "health"));
        assertEquals(3L, value(release, "health.total"), "a component is one summand, however many it folds");

        set(pod, health, 0L);
        set(query, metrics, 1L);   // what is said of it
        assertEquals(1L, value(query, "health"));
        assertEquals(1L, value(release, "health"));
        assertRejected(() -> structure.run(() -> structure.derive(
                query.id(), metrics, Fold.MAX, Over.children(health, "pod"))).join());
    }

    private Object value(final StructureObject object, final String key) {
        return structure.snapshotObject(object.id()).join().get(key);
    }

    private void set(final StructureObject object, final int keyId, final long value) {
        structure.run(() -> structure.setLong(object.id(), keyId, value)).join();
    }

    private static void assertRejected(final Runnable call) {
        final Throwable cause = assertThrows(CompletionException.class, call::run).getCause();
        assertTrue(cause instanceof IllegalArgumentException, String.valueOf(cause));
    }
}
