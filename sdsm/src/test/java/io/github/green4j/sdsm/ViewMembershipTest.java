package io.github.green4j.sdsm;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A subscriber gets one snapshot of what the view holds now, then deltas - and membership follows
 * the properties the selector reads, so an object can enter and leave a view without being created
 * or removed.
 */
class ViewMembershipTest {
    private final StructureRuntime runtime = StructureRuntime.create(1, 1, "view");
    private final Structure structure = runtime.newStructure();

    private final Recorder recorder = new Recorder();

    @AfterEach
    void tearDown() {
        runtime.close();
    }

    private View liveServices() {
        final Set<String> keys = new LinkedHashSet<>();
        keys.add("status");
        return structure.createView("live", "node[status=up]", keys,
                DeliveryPolicy.onChange()).join();
    }

    private void subscribeTo(final View view) {
        structure.subscribe(view.id(), recorder).join();
    }

    @Test
    void shouldOpenWithASnapshotOfWhatIsAlreadyThere() {
        final Node alpha = structure.submit(() -> structure.createNode("Alpha", "service")).join();
        Props.setText(structure, alpha.id(), "status", "up");
        structure.submit(() -> structure.createNode("Beta", "service")).join();

        subscribeTo(liveServices());

        assertEquals(1, recorder.batches());
        assertEquals(List.of(alpha.id()), recorder.idsInSnapshot(ChangeKind.ADDED));
    }

    @Test
    void shouldHoldANodeOnceItHoldsOnAnAxis() {
        final Node region = structure.submit(() -> structure.createNode("eu", "region")).join();
        final Node silo = structure.submit(() -> structure.createNode("silo-1", "silo")).join();
        subscribeTo(structure.createView("holding", "node[axis=placement]", null,
                DeliveryPolicy.onChange()).join());

        structure.run(() -> structure.contain(region.id(), silo.id(), "placement")).join();

        assertEquals(List.of(region.id()), recorder.idsAfterSnapshot(ChangeKind.ADDED));
    }

    @Test
    void shouldDeliverTheSnapshotEvenWhenTheViewIsEmpty() {
        subscribeTo(liveServices());

        assertEquals(1, recorder.batches());
        assertTrue(recorder.idsInSnapshot(ChangeKind.ADDED).isEmpty());
    }

    @Test
    void shouldAddAnObjectWhoseChangedPropertyMakesItMatch() {
        subscribeTo(liveServices());
        final Node alpha = structure.submit(() -> structure.createNode("Alpha", "service")).join();
        Props.setText(structure, alpha.id(), "status", "up");
        structure.flushAll().join();

        assertEquals(List.of(alpha.id()), recorder.idsAfterSnapshot(ChangeKind.ADDED));
    }

    @Test
    void shouldRemoveAnObjectWhichStopsMatchingWithoutRemovingIt() {
        final Node alpha = structure.submit(() -> structure.createNode("Alpha", "service")).join();
        Props.setText(structure, alpha.id(), "status", "up");
        subscribeTo(liveServices());

        Props.setText(structure, alpha.id(), "status", "down");
        structure.flushAll().join();

        assertEquals(List.of(alpha.id()), recorder.idsAfterSnapshot(ChangeKind.REMOVED),
                "left the view");
        assertEquals(1, structure.matchedObjectIds("*[id=" + alpha.id() + "]").join().length,
                "but not the structure");
    }

    /**
     * A key filter holds back what an object reports, not what it is: the intrinsics come
     * either way, because a receiver that does not know a node's name has not been sent a
     * node.
     */
    @Test
    void shouldDeliverOnlyThePropertyKeysTheViewAskedFor() {
        final Node alpha = structure.submit(() -> structure.createNode("Alpha", "service")).join();
        Props.setText(structure, alpha.id(), "status", "up");
        Props.setLong(structure, alpha.id(), "backlog", 84L);

        subscribeTo(liveServices());

        assertEquals(List.of("id", "name", "type", "kind", "status"), recorder.keysInSnapshot());
    }

    @Test
    void shouldKeepItsKeysWhenNewOnesAreRefused() {
        final Node alpha = structure.submit(() -> structure.createNode("Alpha", "service")).join();
        Props.setText(structure, alpha.id(), "status", "up");
        final View live = liveServices();
        subscribeTo(live);

        assertThrows(CompletionException.class, () -> structure.setViewPropertyKeys(live.id(),
                new LinkedHashSet<>(List.of("backlog", " "))).join());
        Props.setLong(structure, alpha.id(), "backlog", 85L);
        structure.flushAll().join();

        assertEquals(0, recorder.recordsFor(alpha.id(), "backlog"));
    }

    @Test
    void shouldDeliverTheValueTheStructureHoldsAtDrainTime() {
        final Node alpha = structure.submit(() -> structure.createNode("Alpha", "service")).join();
        Props.setText(structure, alpha.id(), "status", "up");
        final View everything = structure.createView("all", "node[status=up]", null,
                DeliveryPolicy.onChange()).join();
        structure.subscribe(everything.id(), recorder).join();

        final int backlog = structure.propertyKeys().idOf("backlog");
        structure.run(() -> {
            structure.setLong(alpha.id(), backlog, 1L);
            structure.setLong(alpha.id(), backlog, 2L);
            structure.setLong(alpha.id(), backlog, 3L);
        }).join();
        structure.flushAll().join();

        assertEquals(1, recorder.recordsFor(alpha.id(), "backlog"),
                "one record: the view owes a property, not a history of it");
        assertEquals("3", recorder.latestValueOf(alpha.id(), "backlog"));
    }
}
