package io.github.green4j.sdsm.example.newa;

import io.github.green4j.sdsm.Structure;
import io.github.green4j.sdsm.StructureRuntime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A view on the air is one name on both sides: made in the structure and findable in the
 * channel together, and taken out of both together.
 */
class ViewChannelTest {

    private static final Duration INTERVAL = Duration.ofMillis(100L);

    private final StructureRuntime runtime = StructureRuntime.create(1, 1, "views");
    private final Structure structure = runtime.newStructure();
    private final ViewChannel channel = new ViewChannel(structure);

    @AfterEach
    void tearDown() {
        channel.close();
        runtime.close();
    }

    @Test
    void shouldRefuseANameAlreadyOnTheAirAndLeaveNoViewBehind() {
        channel.publish("streams", "node", null, INTERVAL).join();

        final Throwable cause = assertThrows(CompletionException.class, () ->
                channel.publish("streams", "*", null, INTERVAL).join()).getCause();
        assertTrue(cause instanceof IllegalStateException, String.valueOf(cause));
        assertEquals(1, views());
    }

    @Test
    void shouldRefuseASelectorItCannotRead() {
        final Throwable cause = assertThrows(CompletionException.class, () ->
                channel.publish("bad", "node[[", null, INTERVAL).join()).getCause();
        assertTrue(cause instanceof IllegalArgumentException, String.valueOf(cause));
        assertNull(channel.getEntitySubscriptions("bad"));
    }

    @Test
    void shouldPutAViewOnTheAirAndTakeItOff() {
        channel.publish("streams", "node[$type=stream]", Set.of("backlog"), INTERVAL).join();
        assertNotNull(channel.getEntitySubscriptions("streams"));
        assertEquals(1, views());

        assertTrue(channel.withdraw("streams").join());
        assertNull(channel.getEntitySubscriptions("streams"));
        assertEquals(0, views());
        assertFalse(channel.withdraw("streams").join());
    }

    private int views() {
        return structure.viewIds().join().length;
    }
}
