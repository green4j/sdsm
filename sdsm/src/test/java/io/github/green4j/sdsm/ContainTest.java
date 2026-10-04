package io.github.green4j.sdsm;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A node holds nodes on an axis - a region its cells, a cell its components, a component what
 * it runs on - and is then what a group is: on paths, under selectors, folding its children.
 */
class ContainTest {

    private static final String PLACEMENT = "placement";
    private static final String DEPLOYMENT = "deployment";

    private final StructureRuntime runtime = StructureRuntime.create(1, 1, "contain");
    private final Structure structure = runtime.newStructure();
    private final int health = structure.propertyKeys().idOf("health");
    private final Recorder recorder = new Recorder();

    private long region;
    private long blue;
    private long green;
    private long billing;
    private long pod;

    @BeforeEach
    void place() {
        region = node("eu-de", "region");
        blue = node("blue", "cell");
        green = node("green", "cell");
        billing = node("billing-0", "billing");
        pod = node("billing-0-a", "pod");
        contain(region, blue, PLACEMENT);
        contain(region, green, PLACEMENT);
        contain(blue, billing, PLACEMENT);
        contain(billing, pod, DEPLOYMENT);
    }

    @AfterEach
    void tearDown() {
        runtime.close();
    }

    @Test
    void shouldGiveWhatItHoldsAPathAndAPlaceUnderIt() {
        assertEquals(Path.parse("/region:eu-de/cell:blue/billing:billing-0"),
                structure.pathOf(billing, PLACEMENT).join());
        assertEquals(Path.parse("/billing:billing-0/pod:billing-0-a"),
                structure.pathOf(pod, DEPLOYMENT).join());
        assertEquals(billing, structure.resolve(PLACEMENT, Path.parse("/eu-de/blue/billing-0")).join());
        assertEquals(List.of(blue, green, billing), matched("under(placement, /eu-de)"));
        assertEquals(List.of(pod), matched("node & under(deployment, /billing-0)"));
        assertEquals(PLACEMENT, structure.snapshotObject(blue).join().get("$axis"));
        assertNull(structure.snapshotObject(pod).join().get("$axis"));
    }

    /**
     * A placement that is refused changes nothing: the parent is as free to hold on any axis as
     * it was, and a child held on one axis is not taken to be held on another.
     */
    @Test
    void shouldChangeNothingWhenAPlacementIsRefused() {
        final long orphan = node("orphan", "cell");
        assertThrows(CompletionException.class, () -> contain(orphan, billing, PLACEMENT),
                "a second parent on the axis");

        contain(orphan, pod, "network");
        assertEquals("network", structure.snapshotObject(orphan).join().get("$axis"));
        assertThrows(CompletionException.class, () -> contain(billing, pod, PLACEMENT),
                "held, but on another axis");
    }

    @Test
    void shouldKeepEveryAxisATree() {
        assertThrows(CompletionException.class, () -> contain(green, billing, PLACEMENT),
                "a second parent on the axis");
        assertThrows(CompletionException.class, () -> contain(billing, region, PLACEMENT),
                "a node holds on one axis");
        assertThrows(CompletionException.class, () -> contain(blue, region, PLACEMENT), "a cycle");
        assertThrows(CompletionException.class, () -> contain(blue, blue, PLACEMENT), "itself");

        contain(blue, billing, PLACEMENT);   // where it is already
        structure.run(() -> structure.uncontain(blue, billing)).join();
        contain(green, billing, PLACEMENT);
        assertEquals(Path.parse("/region:eu-de/cell:green/billing:billing-0"),
                structure.pathOf(billing, PLACEMENT).join());
    }

    /**
     * Nothing is under itself however far round it goes, on one axis or across several - and a
     * view reading placement is no reason to walk the loop that was refused.
     */
    @Test
    void shouldRefuseACycleHoweverLong() {
        final long a = node("a", "region");
        final long b = node("b", "cluster");
        final long c = node("c", "release");
        final long d = node("d", "stage");
        structure.createView("placed", "under(placement, /a)", null, DeliveryPolicy.onChange()).join();
        contain(a, b, PLACEMENT);
        contain(b, c, PLACEMENT);
        assertThrows(CompletionException.class, () -> contain(c, a, PLACEMENT), "round one axis");

        contain(c, d, "stage");
        assertThrows(CompletionException.class, () -> contain(d, a, "runtime"), "round three");
        assertEquals(0, structure.parents(a).join().length);
    }

    @Test
    void shouldHoldOneNodeOnEveryAxis() {
        final long stage = node("process", "stage");
        final long colour = node("blue", "colour");
        contain(stage, pod, "stage");
        contain(colour, pod, "colour");

        assertEquals(List.of(billing, stage, colour),
                Arrays.stream(structure.parents(pod).join()).boxed().collect(Collectors.toList()));
    }

    @Test
    void shouldFoldWhatItHoldsAllTheWayUp() {
        final long query = node("todayquery-0", "todayquery");
        contain(green, query, PLACEMENT);
        structure.run(() -> {
            for (final long holder : new long[]{region, blue, green}) {
                structure.derive(holder, health, Fold.MAX, Over.children(health, null));
            }
        }).join();
        set(billing, 1L);
        set(query, 2L);

        assertEquals(1L, value(blue));
        assertEquals(2L, value(region));

        set(query, 0L);
        assertEquals(1L, value(region));
        structure.run(() -> structure.uncontain(blue, billing)).join();
        assertNull(value(blue));
        assertEquals(0L, value(region));
    }

    @Test
    void shouldTellAViewWhatHoldsWhat() {
        structure.subscribe(structure.createView("tree", "node", Set.of(),
                DeliveryPolicy.onChange()).join().id(), recorder).join();
        assertTrue(recorder.membership(ChangeKind.CONTAINED).contains(pod + " in " + billing));

        structure.run(() -> structure.remove(billing)).join();
        structure.flushAll().join();

        assertEquals(List.of(billing), recorder.idsAfterSnapshot(ChangeKind.REMOVED));
        assertTrue(recorder.membership(ChangeKind.UNCONTAINED).contains(pod + " in " + billing));
        assertNull(structure.pathOf(pod, DEPLOYMENT).join(), "what it held is on its own");
        assertTrue(recorder.everyPairFollowsItsEnds());
    }

    @Test
    void shouldFollowAMoveBetweenHolders() {
        structure.subscribe(structure.createView("green", "node & under(placement, /eu-de/green)", Set.of(),
                DeliveryPolicy.onChange()).join().id(), recorder).join();
        assertEquals(List.of(), recorder.idsInSnapshot(ChangeKind.ADDED));

        structure.run(() -> structure.uncontain(blue, billing)).join();
        contain(green, billing, PLACEMENT);
        structure.flushAll().join();

        assertEquals(List.of(billing), recorder.idsAfterSnapshot(ChangeKind.ADDED));
    }

    private long node(final String name, final String type) {
        return structure.submit(() -> structure.createNode(name, type)).join().id();
    }

    private void contain(final long parent, final long child, final String axis) {
        structure.run(() -> structure.contain(parent, child, axis)).join();
    }

    private void set(final long object, final long value) {
        structure.run(() -> structure.setLong(object, health, value)).join();
    }

    private Object value(final long object) {
        return structure.snapshotObject(object).join().get("health");
    }

    private List<Long> matched(final String selector) {
        final long[] ids = structure.matchedObjectIds(selector).join();
        Arrays.sort(ids);
        return Arrays.stream(ids).boxed().collect(Collectors.toList());
    }
}
