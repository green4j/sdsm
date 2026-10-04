package io.github.green4j.sdsm.example.newa;

import io.github.green4j.sdsm.DetailLevel;
import io.github.green4j.sdsm.Structure;
import io.github.green4j.sdsm.StructureRuntime;
import io.netty.channel.ChannelId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * What a client asks for has to reach the far side of the model: a frame read on a session's
 * event loop becomes demand on an object, and demand is what a source paying per query acts
 * on. The frame names things the way the watched world does, so nothing but the structure's
 * own thread can turn a name into an object.
 */
class ClientFramesTest {

    private final StructureRuntime runtime = StructureRuntime.create(1, 1, "newa");
    private final Structure structure = runtime.newStructure();
    private final ViewChannel channel = new ViewChannel(structure);
    private final ClientFrames frames = new ClientFrames(structure, channel);
    private final InterestFrame frame = new InterestFrame();

    @AfterEach
    void tearDown() {
        runtime.close();
    }

    @Test
    void shouldReadWhatTheClientSays() {
        assertTrue(frame.read("{\"op\":\"subscribe\",\"view\":\"topology\"}"));
        assertEquals("subscribe", frame.op());
        assertEquals("topology", frame.view());

        assertTrue(frame.read("{\"op\":\"interest\",\"base\":\"COARSE\","
                + "\"overrides\":{\"eu-de-1/blue/billing\":\"OFF\"}}"));
        assertEquals("interest", frame.op());
        assertEquals(DetailLevel.COARSE, frame.base());
        assertEquals(1, frame.overrideCount());
        assertEquals("eu-de-1/blue/billing", frame.overrideIdAt(0));
        assertEquals(DetailLevel.OFF, frame.overrideLevelAt(0));

        assertFalse(frame.read("{\"op\":"), "half a frame is not a frame");
    }

    /**
     * A level nobody has heard of is not the finest one: a typo would have the sources fetch
     * everything, and the client would never know.
     */
    @Test
    void shouldRefuseALevelThereIsNone() {
        assertFalse(frame.read("{\"op\":\"interest\",\"base\":\"COURSE\"}"));
        assertFalse(frame.read("{\"op\":\"interest\",\"overrides\":{\"a\":\"OF\"}}"));
        assertTrue(frame.read("{\"op\":\"interest\",\"base\":\"coarse\"}"));
        assertEquals(DetailLevel.COARSE, frame.base());
    }

    /**
     * The base reaches everything, an override reaches the one thing it names, and a name
     * nothing answers to is not an error - a client may be looking at what has just gone.
     */
    @Test
    void shouldTurnAFrameIntoDemandOnTheObjectsItNames() {
        structure.run(() -> {
            structure.createNode("billing", "pod", "eu-de-1/blue/billing");
            structure.createNode("orders", "stream", "eu-de-1/blue/orders");
        }).join();

        frame.read("{\"op\":\"interest\",\"base\":\"COARSE\",\"overrides\":{"
                + "\"eu-de-1/blue/billing\":\"OFF\","
                + "\"eu-de-1/blue/nothing-here\":\"FINE\"}}");
        frames.wants("ws-1", frame);

        awaitTrue(() -> levelOf("eu-de-1/blue/billing") == DetailLevel.OFF);
        assertEquals(DetailLevel.COARSE, levelOf("eu-de-1/blue/orders"));
    }

    /**
     * The short form of a channel id is four random bytes: among thousands of windows two of
     * them share it, and would share one interest.
     */
    @Test
    void shouldNameEveryConnectionApart() {
        assertNotEquals(ClientFrames.clientOf(channelId("0a0b0c0d", "first")),
                ClientFrames.clientOf(channelId("0a0b0c0d", "second")));
    }

    private static ChannelId channelId(final String shortText, final String longText) {
        return new ChannelId() {
            @Override
            public String asShortText() {
                return shortText;
            }

            @Override
            public String asLongText() {
                return longText;
            }

            @Override
            public int compareTo(final ChannelId other) {
                return asLongText().compareTo(other.asLongText());
            }
        };
    }

    private DetailLevel levelOf(final String externalId) {
        return structure.submit(
                () -> structure.findByExternalId(externalId).requiredLevel()).join();
    }

    private static void awaitTrue(final BooleanSupplier condition) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                fail("the demand never got there");
            }
            Thread.yield();
        }
    }
}
