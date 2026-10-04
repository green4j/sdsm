package io.github.green4j.sdsm;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Links are selected by their ends: {@code between(e)} holds a link whose two nodes {@code e}
 * holds, {@code touching(e)} one with either, and both follow the nodes as they change. A link a
 * view carries comes with its two ports, since a link names its ends by port and a port says
 * which node it is on; the node at the far end is known by that id and not delivered.
 */
class BetweenTest {
    private static final String ZONE = "node[zone=in]";

    private final StructureRuntime runtime = StructureRuntime.create(1, 1, "between");
    private final Structure structure = runtime.newStructure();

    private final Node alpha = structure.submit(() -> structure.createNode("Alpha", "service")).join();
    private final Node beta = structure.submit(() -> structure.createNode("Beta", "service")).join();
    private final Node gamma = structure.submit(() -> structure.createNode("Gamma", "service")).join();

    private final Link alphaToBeta = link(alpha, beta);
    private final Link betaToGamma = link(beta, gamma);

    @AfterEach
    void tearDown() {
        runtime.close();
    }

    private Link link(final Node from, final Node to) {
        return structure.submit(() -> {
            final Output out = structure.addOutput(from.id(), "out-" + to.name(), "tcp");
            final Input in = structure.addInput(to.id(), "in-" + from.name(), "tcp");
            return structure.createLink(from.name() + "->" + to.name(), "tcp", out.id(), in.id());
        }).join();
    }

    private void select(final Node node) {
        Props.setText(structure, node.id(), "zone", "in");
    }

    private View viewWith(final String links) {
        return structure.createView("zone", links == null ? ZONE : ZONE + ", " + links, Set.of("zone"),
                DeliveryPolicy.onChange()).join();
    }

    private Recorder subscribedTo(final View view) {
        final Recorder recorder = new Recorder();
        structure.subscribe(view.id(), recorder).join();
        return recorder;
    }

    private List<Long> membersOf(final View view) {
        return subscribedTo(view).idsInSnapshot(ChangeKind.ADDED);
    }

    private static Set<Long> withPorts(final Link link, final Long... others) {
        final Set<Long> ids = new HashSet<>(List.of(others));
        ids.add(link.id());
        ids.add(link.fromOutput().id());
        ids.add(link.toInput().id());
        return ids;
    }

    @Test
    void shouldCarryNoLinksItIsNotAskedFor() {
        select(alpha);
        select(beta);

        assertEquals(List.of(alpha.id(), beta.id()), membersOf(viewWith(null)));
    }

    @Test
    void shouldCarryALinkOnlyWhenBothItsNodesAreSelected() {
        select(alpha);
        select(beta);

        assertEquals(withPorts(alphaToBeta, alpha.id(), beta.id()),
                new HashSet<>(membersOf(viewWith("between(" + ZONE + ")"))));
    }

    @Test
    void shouldCarryALinkWithOneSelectedNode() {
        select(alpha);

        assertEquals(withPorts(alphaToBeta, alpha.id()),
                new HashSet<>(membersOf(viewWith("touching(" + ZONE + ")"))));
    }

    @Test
    void shouldLetThePortsGoWithTheirLastLink() {
        select(alpha);
        final Recorder recorder = subscribedTo(viewWith("touching(" + ZONE + ")"));

        Props.setText(structure, alpha.id(), "zone", "out");
        structure.flushAll().join();

        assertEquals(withPorts(alphaToBeta, alpha.id()),
                new HashSet<>(recorder.idsAfterSnapshot(ChangeKind.REMOVED)));
    }

    @Test
    void shouldCarryTheLinkADeclarationDrawsWithItsPorts() {
        select(alpha);
        final Recorder recorder = subscribedTo(viewWith("touching(" + ZONE + ")"));

        final Output out = structure.submit(() -> structure.addOutput(alpha.id(), "out-feed", "tcp")).join();
        final Input in = structure.submit(() -> structure.addInput(gamma.id(), "in-feed", "tcp")).join();
        structure.run(() -> {
            structure.provide(out.id(), "feed");
            structure.require(in.id(), "feed");
        }).join();
        structure.flushAll().join();

        final List<Long> added = recorder.idsAfterSnapshot(ChangeKind.ADDED);
        assertEquals(3, added.size(), added.toString());
        assertTrue(added.containsAll(List.of(out.id(), in.id())), added.toString());
    }

    @Test
    void shouldTakeALinkInWhenItsSecondNodeIsSelected() {
        select(alpha);
        final Recorder recorder = subscribedTo(viewWith("between(" + ZONE + ")"));

        select(beta);
        structure.flushAll().join();

        assertEquals(withPorts(alphaToBeta, beta.id()),
                new HashSet<>(recorder.idsAfterSnapshot(ChangeKind.ADDED)));
    }

    @Test
    void shouldDropALinkWhenANodeLeavesTheSelector() {
        select(alpha);
        select(beta);
        select(gamma);
        final Recorder recorder = subscribedTo(viewWith("between(" + ZONE + ")"));

        Props.setText(structure, gamma.id(), "zone", "out");
        structure.flushAll().join();

        assertEquals(withPorts(betaToGamma, gamma.id()),
                new HashSet<>(recorder.idsAfterSnapshot(ChangeKind.REMOVED)));
    }

    @Test
    void shouldFollowTheEndsAsTheyMoveBetweenParents() {
        final Node blue = structure.submit(() -> structure.createNode("blue", "cell")).join();
        final Node green = structure.submit(() -> structure.createNode("green", "cell")).join();
        for (final Node node : List.of(alpha, beta, gamma)) {
            structure.run(() -> structure.contain(blue.id(), node.id(), "placement")).join();
        }
        final Recorder recorder = subscribedTo(structure.createView("blue links",
                "between(node & under(placement, /blue))", null, DeliveryPolicy.onChange()).join());
        assertEquals(withPorts(betaToGamma, alphaToBeta.id(), alphaToBeta.fromOutput().id(),
                alphaToBeta.toInput().id()), new HashSet<>(recorder.idsInSnapshot(ChangeKind.ADDED)));

        structure.run(() -> structure.uncontain(blue.id(), gamma.id())).join();
        structure.run(() -> structure.contain(green.id(), gamma.id(), "placement")).join();
        structure.flushAll().join();

        assertEquals(withPorts(betaToGamma), new HashSet<>(recorder.idsAfterSnapshot(ChangeKind.REMOVED)));
    }
}
