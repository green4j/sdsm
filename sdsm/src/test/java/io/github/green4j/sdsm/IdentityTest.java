package io.github.green4j.sdsm;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * An object mirrors something that has its own identity elsewhere. Observing that thing again
 * has to reach the object already standing for it, or the structure grows a second copy of the
 * world every time an observation repeats.
 */
class IdentityTest {
    private final StructureRuntime runtime = StructureRuntime.create(1, 1, "identity");
    private final Structure structure = runtime.newStructure();

    @AfterEach
    void tearDown() {
        runtime.close();
    }

    private static Throwable causeOf(final Executable call) {
        return assertThrows(CompletionException.class, call).getCause();
    }

    /**
     * What a binding does for every observation: resolve the key, and only make an object when
     * nothing answers to it. One task, so no second observation can slip in between.
     *
     * @param externalId the key the observed thing is known by
     * @param podName     name to give the object if it has to be made
     * @return id of the object standing for that thing
     */
    private long observePod(final String externalId, final String podName) {
        return structure.submit(() -> {
            final StructureObject known = structure.findByExternalId(externalId);
            if (known != null) {
                return Long.valueOf(known.id());
            }
            final Node made = structure.createNode(podName, "pod", externalId);
            return Long.valueOf(made.id());
        }).join().longValue();
    }

    @Test
    void shouldAnswerASecondObservationWithTheObjectItAlreadyMade() {
        final long first = observePod("pod-uid-1", "ingest-0");
        final long second = observePod("pod-uid-1", "ingest-0");

        assertEquals(first, second);
        assertEquals(1, structure.matchedObjectIds("node").join().length);
    }

    @Test
    void shouldRefuseToGiveOneKeyToTwoObjects() {
        final long held = observePod("pod-uid-1", "ingest-0");

        final Throwable cause = causeOf(
                () -> structure.submit(() -> structure.createNode("ingest-1", "pod", "pod-uid-1")).join());

        assertTrue(cause instanceof IllegalArgumentException, String.valueOf(cause));
        assertTrue(cause.getMessage().contains(Long.toString(held)), cause.getMessage());
    }

    @Test
    void shouldFreeTheKeyWhenTheObjectGoes() {
        final long first = observePod("pod-uid-1", "ingest-0");
        structure.run(() -> structure.remove(first)).join();

        final long second = observePod("pod-uid-1", "ingest-0");

        assertNotEquals(first, second);
        assertEquals(1, structure.matchedObjectIds("node").join().length);
    }

    @Test
    void shouldFreeTheKeyOfAnObjectRemovedInACascade() {
        final Node node = structure.submit(() -> structure.createNode("ingest-0", "pod", "pod-uid-1")).join();
        structure.submit(() -> structure.addOutput(node.id(), "out", "tcp")).join();
        assertNotNull(structure.submit(() -> structure.findByExternalId("pod-uid-1>out")).join(),
                "a port is known by its node's id, its side and its name");

        structure.run(() -> structure.remove(node.id())).join();

        assertNull(structure.submit(() -> structure.findByExternalId("pod-uid-1>out")).join());
    }

    @Test
    void shouldCarryTheKeyAsAPropertyToSelectBy() {
        final long known = observePod("pod-uid-1", "ingest-0");
        observePod("pod-uid-2", "ingest-1");

        assertEquals("pod-uid-1", structure.snapshotObject(known).join().get("$externalId"));
        assertArrayEquals(new long[]{known},
                structure.matchedObjectIds("node[$externalId=pod-uid-1]").join());
    }

    @Test
    void shouldRefuseAPropertyWriteUnderTheKeyOfAnIntrinsic() {
        final long known = observePod("pod-uid-1", "ingest-0");

        final Throwable cause = causeOf(() -> structure.run(
                () -> structure.setText(known, PropertyKeys.EXTERNAL_ID, "pod-uid-2")).join());

        assertTrue(cause instanceof IllegalArgumentException, String.valueOf(cause));
    }

    /**
     * A binding takes its observations from a buffer, not from Strings it made, so the
     * structure reads an id as characters and a reused StringBuilder resolves it.
     */
    @Test
    void shouldResolveAnIdSpelledByAnyCharSequence() {
        final long known = observePod("pod-uid-1", "api");

        final StringBuilder probe = new StringBuilder("pod-uid-1");
        final StructureObject found = structure.submit(
                () -> structure.findByExternalId(probe)).join();

        assertNotNull(found);
        assertEquals(known, found.id());
    }
}
