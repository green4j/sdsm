package io.github.green4j.sdsm;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * An object is found on an axis by the names of its groups, typed where two share a name, or by
 * their ids, and every object on an axis has one path by typed names back.
 */
class PathTest {

    private final StructureRuntime runtime = StructureRuntime.create(1, 1, "path");
    private final Structure structure = runtime.newStructure();

    @AfterEach
    void tearDown() {
        runtime.close();
    }

    @Test
    void shouldSpellWhatItParses() {
        final Path path = Path.parse("/region:eu-de/#57/ingest-0");

        assertEquals(3, path.length());
        assertEquals("region", path.typeAt(0));
        assertEquals("eu-de", path.nameAt(0));
        assertEquals(57L, path.idAt(1));
        assertNull(path.nameAt(1));
        assertNull(path.typeAt(2));
        assertEquals("/region:eu-de/#57/ingest-0", path.toString());
        assertEquals(path, Path.parse("region:eu-de/#57/ingest-0"));
        assertEquals(Path.root(), Path.parse("/"));
    }

    @Test
    void shouldEscapeWhatWouldBreakASegment() {
        final Path path = Path.root().child("port", "dxtick://tb:8011/CHANNEL_1").child(null, "#1");

        assertEquals("/port:dxtick\\:\\/\\/tb\\:8011\\/CHANNEL_1/\\#1", path.toString());
        assertEquals(path, Path.parse(path.toString()));
        assertEquals("#1", Path.parse(path.toString()).nameAt(1));
    }

    @Test
    void shouldRefuseWhatIsNotAPath() {
        assertThrows(IllegalArgumentException.class, () -> Path.parse("/eu-de//ingest-0"));
        assertThrows(IllegalArgumentException.class, () -> Path.parse("/:ingest-0"));
        assertThrows(IllegalArgumentException.class, () -> Path.parse("/#five"));
        assertThrows(IllegalArgumentException.class, () -> Path.parse("/ingest-0\\"));
    }

    @Test
    void shouldFindAnObjectByNamesByTypesAndByIds() {
        final Placed placed = placed();

        assertEquals(placed.pod, resolve("placement", "/eu-de/blue/ingest-0"));
        assertEquals(placed.pod, resolve("placement", "/region:eu-de/silo:blue/pod:ingest-0"));
        assertEquals(placed.pod, resolve("placement", "/eu-de/#" + placed.blue + "/ingest-0"));
        assertEquals(placed.blue, resolve("placement", "/eu-de/blue"));
        assertEquals(-1L, resolve("placement", "/eu-de/green/ingest-0"));
        assertEquals(-1L, resolve("placement", "/eu-de/blue/ingest-0/more"));
        assertEquals(-1L, resolve("placement", "/eu-de/#" + placed.pod));
        assertEquals(-1L, resolve("stage", "/eu-de/blue/ingest-0"));
    }

    @Test
    void shouldAskForTheTypeWhereTwoShareAName() {
        final Placed placed = placed();
        final long service = structure.submit(() -> structure.createNode("ingest-0", "service")).join().id();
        structure.run(() -> structure.contain(placed.blue, service, "placement")).join();

        final Throwable cause = assertThrows(CompletionException.class,
                () -> resolve("placement", "/eu-de/blue/ingest-0")).getCause();
        assertTrue(cause instanceof IllegalArgumentException, String.valueOf(cause));
        assertEquals(service, resolve("placement", "/eu-de/blue/service:ingest-0"));
        assertEquals(placed.pod, resolve("placement", "/eu-de/blue/pod:ingest-0"));
    }

    @Test
    void shouldGiveEveryObjectItsPathOnEachAxis() {
        final Placed placed = placed();
        final long stage = structure.submit(() -> structure.createNode("ingest", "stage")).join().id();
        structure.run(() -> structure.contain(stage, placed.pod, "stage")).join();

        assertEquals(Path.parse("/region:eu-de/silo:blue/pod:ingest-0"),
                structure.pathOf(placed.pod, "placement").join());
        assertEquals(Path.parse("/stage:ingest/pod:ingest-0"),
                structure.pathOf(placed.pod, "stage").join());
        assertEquals(Path.parse("/region:eu-de/silo:blue"),
                structure.pathOf(placed.blue, "placement").join());
        assertNull(structure.pathOf(placed.pod, "deployment").join());
        final Path canonical = structure.pathOf(placed.pod, "placement").join();
        assertEquals(placed.pod, resolve("placement", canonical.toString()));
    }

    private static final class Placed {
        private final long blue;
        private final long pod;

        Placed(final long blue, final long pod) {
            this.blue = blue;
            this.pod = pod;
        }
    }

    private Placed placed() {
        final long region = structure.submit(() -> structure.createNode("eu-de", "region")).join().id();
        final long blue = structure.submit(() -> structure.createNode("blue", "silo")).join().id();
        final long green = structure.submit(() -> structure.createNode("green", "silo")).join().id();
        structure.run(() -> structure.contain(region, blue, "placement")).join();
        structure.run(() -> structure.contain(region, green, "placement")).join();
        final long pod = structure.submit(() -> structure.createNode("ingest-0", "pod")).join().id();
        structure.run(() -> structure.contain(blue, pod, "placement")).join();
        return new Placed(blue, pod);
    }

    private long resolve(final String axis, final String path) {
        return structure.resolve(axis, Path.parse(path)).join();
    }
}
