package io.github.green4j.sdsm;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A task is the transaction. Everything it mutates - including what it reaches through the
 * future-returning operations, which run there and then rather than queueing behind it -
 * belongs to one batch, and every property it writes is attributed to one source.
 */
class TransactionTest {
    private static final int KUBERNETES = 1;
    private static final int DATADOG = 2;

    private final StructureRuntime runtime = StructureRuntime.create(1, 1, "transaction");
    private final Structure structure = runtime.newStructure();

    private final Recorder recorder = new Recorder();

    @AfterEach
    void tearDown() {
        runtime.close();
    }

    private View allPods() {
        final Set<String> keys = new LinkedHashSet<>();
        keys.add("rate");
        final View view = structure.createView("pods", "node[type=pod]", keys,
                DeliveryPolicy.onChange()).join();
        structure.subscribe(view.id(), recorder).join();
        return view;
    }

    @Test
    void shouldDeliverASubtreeBuiltInOneTaskAsOneBatch() {
        allPods();
        final int rate = structure.propertyKeys().idOf("rate");

        structure.run(() -> {
            final Node from = structure.createNode("ingest-0", "pod");
            final Node to = structure.createNode("aggregate-0", "pod");
            final Output out = structure.addOutput(from.id(), "out", "tcp");
            final Input in = structure.addInput(to.id(), "in", "tcp");
            structure.createLink("ingest->aggregate", "tcp", out.id(), in.id());
            structure.setLong(from.id(), rate, 12_000L);
            structure.setLong(to.id(), rate, 4_000L);
        }).join();

        assertEquals(2, recorder.batches(), "a snapshot and one delta");
        assertEquals(2, recorder.idsAfterSnapshot(ChangeKind.ADDED).size());
    }

    @Test
    void shouldLeaveAPropertyNobodyClaimedUnattributed() {
        final Node pod = structure.submit(() -> structure.createNode("ingest-0", "pod")).join();
        final int rate = structure.propertyKeys().idOf("rate");

        structure.run(() -> structure.setLong(pod.id(), rate, 12_000L)).join();

        assertEquals(Structure.NO_SOURCE, sourceOf(pod, rate));
    }

    @Test
    void shouldCarryTheAttributionIntoWhateverTheTaskCalls() {
        final Node pod = structure.submit(() -> structure.createNode("ingest-0", "pod")).join();
        final int rate = structure.propertyKeys().idOf("rate");
        final int state = structure.propertyKeys().idOf("state");

        structure.run(KUBERNETES, () -> {
            structure.run(() -> structure.setText(pod.id(), state, "Running")).join();
            structure.run(DATADOG, () -> structure.setLong(pod.id(), rate, 12_000L)).join();
            structure.setText(pod.id(), state, "Ready");
        }).join();

        assertEquals(KUBERNETES, sourceOf(pod, state));
        assertEquals(DATADOG, sourceOf(pod, rate));
    }

    private int sourceOf(final StructureObject object, final int keyId) {
        return structure.submit(() -> Integer.valueOf(object.sourceOf(keyId)))
                .join().intValue();
    }
}
