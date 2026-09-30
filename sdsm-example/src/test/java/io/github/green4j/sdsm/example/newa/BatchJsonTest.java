package io.github.green4j.sdsm.example.newa;

import io.github.green4j.jelly.ByteArray;
import io.github.green4j.sdsm.BatchSubscriber;
import io.github.green4j.sdsm.DeliveryPolicy;
import io.github.green4j.sdsm.Node;
import io.github.green4j.sdsm.Structure;
import io.github.green4j.sdsm.StructureBatch;
import io.github.green4j.sdsm.StructureRuntime;
import io.github.green4j.sdsm.View;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What a client on the other end of the socket is actually sent. The frame has to carry the
 * whole of a batch - what was added, what a property now is, and who is in what - because that
 * is all a client has: it holds no structure and cannot ask.
 */
class BatchJsonTest {

    private final StructureRuntime runtime = StructureRuntime.create(1, 1, "newa");
    private final Structure structure = runtime.newStructure();
    private final BatchJson frames = new BatchJson("topology");
    private final List<String> sent = new ArrayList<>();

    private final BatchSubscriber rendering = new BatchSubscriber() {
        @Override
        public void onBatch(final StructureBatch batch) {
            sent.add(textOf(frames.render(batch)));
        }

        @Override
        public void onError(final Throwable reason) {
            failure = reason;
        }
    };

    private volatile Throwable failure;

    @AfterEach
    void tearDown() {
        runtime.close();
    }

    @Test
    void shouldCarryASnapshotWithEverythingInIt() {
        final Node pod =
                structure.submit(() -> structure.createNode("aggregator", "pod", "eu-de-1/blue/aggregator")).join();
        final Node release = structure.submit(() -> structure.createNode("blue", "release")).join();
        structure.run(() -> structure.contain(release.id(), pod.id(), "placement")).join();
        subscribe();

        noFailure();
        final String frame = sent.get(0);
        assertTrue(frame.startsWith("{\"view\":\"topology\","), frame);
        assertTrue(frame.contains("\"snapshot\":true"), frame);
        assertTrue(frame.contains("{\"what\":\"ADDED\",\"id\":" + pod.id()
                + ",\"kind\":\"NODE\"}"), frame);
        final List<String> strings = Json.stringsIn(frame);
        assertEquals("eu-de-1/blue/aggregator", strings.get(strings.indexOf("externalId") + 2), frame);
        assertTrue(frame.contains("{\"what\":\"CONTAINED\",\"id\":" + pod.id()
                + ",\"parent\":" + release.id() + "}"), frame);
    }

    @Test
    void shouldCarryEachValueAsWhatItIs() {
        final Node pod = structure.submit(() -> structure.createNode("aggregator", "pod")).join();
        subscribe();
        final int pods = structure.propertyKeys().idOf("pods");
        final int rate = structure.propertyKeys().idOf("inRate");
        final int live = structure.propertyKeys().idOf("live");
        structure.run(() -> {
            structure.setLong(pod.id(), pods, 3L);
            structure.setDouble(pod.id(), rate, 12000.5);
            structure.setBoolean(pod.id(), live, true);
        }).join();
        structure.flushAll().join();

        noFailure();
        final String frame = sent.get(sent.size() - 1);
        assertEquals(2, sent.size(), "one snapshot and one delta");
        assertTrue(frame.contains("\"snapshot\":false"), frame);
        assertTrue(frame.contains("\"key\":\"pods\",\"value\":3"), frame);
        assertTrue(frame.contains("\"key\":\"inRate\",\"value\":\"12000.5\""), frame);
        assertTrue(frame.contains("\"key\":\"live\",\"value\":true"), frame);
    }

    @Test
    void shouldSayAPropertyIsGoneRatherThanZero() {
        final Node pod = structure.submit(() -> structure.createNode("stream", "stream")).join();
        final int backlog = structure.propertyKeys().idOf("backlog");
        structure.run(() -> structure.setLong(pod.id(), backlog, 84L)).join();
        subscribe();
        structure.run(() -> structure.removeProperty(pod.id(), backlog)).join();
        structure.flushAll().join();

        noFailure();
        assertTrue(sent.get(sent.size() - 1).contains("\"key\":\"backlog\",\"value\":null"),
                sent.toString());
    }

    /**
     * Text is whatever a source wrote, so a frame quotes it rather than splicing it in.
     */
    @Test
    void shouldQuoteWhateverTextHolds() {
        final String awkward = "say \"hi\"\\ /\n\t\u00e9 \u0436\u2713";
        final Node pod = structure.submit(() -> structure.createNode("a\"b\u0436", "pod")).join();
        subscribe();
        final int state = structure.propertyKeys().idOf("state");
        structure.run(() -> structure.setText(pod.id(), state, awkward)).join();
        structure.flushAll().join();

        noFailure();
        assertTrue(Json.stringsIn(sent.get(0)).contains("a\"b\u0436"), sent.get(0));
        assertTrue(Json.stringsIn(sent.get(sent.size() - 1)).contains(awkward), sent.toString());
    }

    private void noFailure() {
        if (failure != null) {
            throw new AssertionError("the view could not deliver", failure);
        }
    }

    private void subscribe() {
        final View view = structure.createView("topology", "*", null,
                DeliveryPolicy.onChange()).join();
        structure.subscribe(view.id(), rendering).join();
    }

    private static String textOf(final ByteArray rendered) {
        return new String(rendered.array(), rendered.start(), rendered.length(),
                StandardCharsets.UTF_8);
    }
}
