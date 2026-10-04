package io.github.green4j.sdsm;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Ports are selected by their node: {@code on(e)} holds the ports of the nodes {@code e} holds,
 * linked or not, and follows the nodes as they change and move.
 */
class OnTest {
    private static final String ZONE = "node[zone=in]";

    private final StructureRuntime runtime = StructureRuntime.create(1, 1, "on");
    private final Structure structure = runtime.newStructure();

    private final Node alpha = structure.submit(() -> structure.createNode("Alpha", "service")).join();
    private final Node beta = structure.submit(() -> structure.createNode("Beta", "service")).join();
    private final Output alphaOut = structure.submit(() -> structure.addOutput(alpha.id(), "out", "tcp")).join();
    private final Input betaIn = structure.submit(() -> structure.addInput(beta.id(), "in", "tcp")).join();

    @AfterEach
    void tearDown() {
        runtime.close();
    }

    private void zone(final Node node, final String zone) {
        Props.setText(structure, node.id(), "zone", zone);
    }

    private Recorder subscribedTo(final String selector) {
        final View view = structure.createView("on", selector, Set.of("zone"), DeliveryPolicy.onChange()).join();
        final Recorder recorder = new Recorder();
        structure.subscribe(view.id(), recorder).join();
        return recorder;
    }

    @Test
    void shouldCarryThePortsOfTheSelectedNodesWithNoLink() {
        zone(alpha, "in");

        assertEquals(List.of(alpha.id(), alphaOut.id()),
                subscribedTo(ZONE + ", on(" + ZONE + ")").idsInSnapshot(ChangeKind.ADDED));
    }

    @Test
    void shouldFollowTheNodesAsTheyChange() {
        zone(alpha, "in");
        final Recorder recorder = subscribedTo("on(" + ZONE + ")");

        zone(alpha, "out");
        zone(beta, "in");
        structure.flushAll().join();

        assertEquals(List.of(alphaOut.id()), recorder.idsAfterSnapshot(ChangeKind.REMOVED));
        assertEquals(List.of(betaIn.id()), recorder.idsAfterSnapshot(ChangeKind.ADDED));
    }

    @Test
    void shouldCarryAPortAddedToASelectedNode() {
        zone(alpha, "in");
        final Recorder recorder = subscribedTo("on(" + ZONE + ")");

        final Input alphaIn = structure.submit(() -> structure.addInput(alpha.id(), "in", "tcp")).join();
        structure.flushAll().join();

        assertEquals(List.of(alphaIn.id()), recorder.idsAfterSnapshot(ChangeKind.ADDED));
    }

    @Test
    void shouldFollowTheNodesAsTheyMoveBetweenParents() {
        final Node blue = structure.submit(() -> structure.createNode("blue", "cell")).join();
        final Node green = structure.submit(() -> structure.createNode("green", "cell")).join();
        structure.run(() -> structure.contain(blue.id(), alpha.id(), "placement")).join();
        final Recorder recorder = subscribedTo("on(node & under(placement, /blue))");
        assertEquals(List.of(alphaOut.id()), recorder.idsInSnapshot(ChangeKind.ADDED));

        structure.run(() -> structure.uncontain(blue.id(), alpha.id())).join();
        structure.run(() -> structure.contain(green.id(), alpha.id(), "placement")).join();
        structure.flushAll().join();

        assertEquals(List.of(alphaOut.id()), recorder.idsAfterSnapshot(ChangeKind.REMOVED));
    }
}
