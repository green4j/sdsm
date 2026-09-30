package io.github.green4j.sdsm;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A view of one part of an axis - a silo, a namespace, a day - selects by where objects are
 * placed rather than by a property copied onto them, and follows them as they move between
 * groups, alone or with the group that carries them.
 */
class UnderTest {

    private final StructureRuntime runtime = StructureRuntime.create(1, 1, "under");
    private final Structure structure = runtime.newStructure();
    private final Recorder recorder = new Recorder();

    private long region;
    private long blue;
    private long green;
    private long day;
    private long ingest;
    private long query;

    @BeforeEach
    void place() {
        region = structure.submit(() -> structure.createNode("eu-de", "region")).join().id();
        blue = structure.submit(() -> structure.createNode("blue", "silo")).join().id();
        green = structure.submit(() -> structure.createNode("green", "silo")).join().id();
        structure.run(() -> structure.contain(region, blue, "placement")).join();
        structure.run(() -> structure.contain(region, green, "placement")).join();
        day = structure.submit(() -> structure.createNode("td-1", "tradingday")).join().id();
        structure.run(() -> structure.contain(green, day, "placement")).join();
        ingest = structure.submit(() -> structure.createNode("ingest-0", "pod")).join().id();
        structure.run(() -> structure.contain(blue, ingest, "placement")).join();
        query = structure.submit(() -> structure.createNode("query-0", "pod")).join().id();
        structure.run(() -> structure.contain(day, query, "placement")).join();
    }

    @AfterEach
    void tearDown() {
        runtime.close();
    }

    @Test
    void shouldSelectWhatLiesUnderAPath() {
        assertEquals(List.of(ingest), matched("node[type=pod] & under(placement, /eu-de/blue)"));
        assertEquals(List.of(ingest, query), matched("node[type=pod] & under(placement, /eu-de/*)"));
        assertEquals(List.of(query), matched("under(placement, /region:eu-de/silo:*/tradingday:*)"));
        assertEquals(List.of(blue, green, day, ingest, query), matched("under(placement, /eu-de)"));
        assertEquals(List.of(ingest), matched("under(placement, /eu-de/#" + blue + ")"));
        assertEquals(List.of(), matched("under(stage, /eu-de)"));
    }

    @Test
    void shouldFollowAnObjectThatMoves() {
        subscribeTo("node[type=pod] & under(placement, /eu-de/green)");
        assertEquals(List.of(query), recorder.idsInSnapshot(ChangeKind.ADDED));

        structure.run(() -> structure.uncontain(blue, ingest)).join();
        structure.run(() -> structure.contain(green, ingest, "placement")).join();
        structure.flushAll().join();
        assertEquals(List.of(ingest), recorder.idsAfterSnapshot(ChangeKind.ADDED));

        structure.run(() -> structure.uncontain(green, ingest)).join();
        structure.flushAll().join();
        assertEquals(List.of(ingest), recorder.idsAfterSnapshot(ChangeKind.REMOVED));
    }

    @Test
    void shouldFollowWhatAGroupCarries() {
        subscribeTo("node[type=pod] & under(placement, /eu-de/blue)");

        structure.run(() -> structure.uncontain(green, day)).join();
        structure.run(() -> structure.contain(blue, day, "placement")).join();
        structure.flushAll().join();

        assertEquals(List.of(query), recorder.idsAfterSnapshot(ChangeKind.ADDED));
    }

    @Test
    void shouldStateAMoveAsWhatItIsBetweenWhatIsThere() {
        subscribeTo("under(placement, /eu-de)");

        structure.run(() -> structure.uncontain(blue, ingest)).join();
        structure.run(() -> structure.contain(green, ingest, "placement")).join();
        structure.flushAll().join();

        // out of a group it is under nothing, so it leaves the view and comes back
        assertEquals(List.of(ingest), recorder.idsAfterSnapshot(ChangeKind.REMOVED));
        assertEquals(List.of(ingest), recorder.idsAfterSnapshot(ChangeKind.ADDED));
        final List<String> joined = recorder.membership(ChangeKind.CONTAINED);
        assertEquals(1, joined.stream().filter((ingest + " in " + green)::equals).count());
        assertTrue(recorder.everyPairFollowsItsEnds());
    }

    private List<Long> matched(final String selector) {
        final long[] ids = structure.matchedObjectIds(selector).join();
        Arrays.sort(ids);
        return Arrays.stream(ids).boxed().collect(Collectors.toList());
    }

    private void subscribeTo(final String selector) {
        final View view = structure.createView("part", selector, Set.of(),
                DeliveryPolicy.onChange()).join();
        structure.subscribe(view.id(), recorder).join();
    }
}
