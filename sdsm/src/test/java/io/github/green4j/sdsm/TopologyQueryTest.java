package io.github.green4j.sdsm;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

/**
 * What a client rebuilding the structure has to ask for by hand: the ends of a link, the ports of
 * a node, the members of a group and the groups holding an object. Without these it would have to
 * select everything and work the shape out itself.
 */
class TopologyQueryTest {
    private final StructureRuntime runtime = StructureRuntime.create(1, 1, "query");
    private final Structure structure = runtime.newStructure();

    private final Node alpha = structure.submit(() -> structure.createNode("Alpha", "service")).join();
    private final Node beta = structure.submit(() -> structure.createNode("Beta", "service")).join();
    private final Output out = structure.submit(() -> structure.addOutput(alpha.id(), "out", "tcp")).join();
    private final Input in = structure.submit(() -> structure.addInput(beta.id(), "in", "tcp")).join();
    private final Link edge =
            structure.submit(() -> structure.createLink("alpha->beta", "tcp", out.id(), in.id())).join();

    @AfterEach
    void tearDown() {
        runtime.close();
    }

    @Test
    void shouldNameBothEndsOfALinkAndTheNodesBehindThem() {
        assertArrayEquals(new long[]{out.id(), alpha.id(), in.id(), beta.id()},
                structure.linkEndpoints(edge.id()).join());
    }

    @Test
    void shouldListThePortsOfANodeByKind() {
        assertArrayEquals(new long[]{out.id()},
                structure.nodePorts(alpha.id(), ObjectKind.OUTPUT).join());
        assertArrayEquals(new long[0],
                structure.nodePorts(alpha.id(), ObjectKind.INPUT).join());
    }

    @Test
    void shouldListWhatAGroupHoldsAndWhatHoldsAnObject() {
        final Node region = structure.submit(() -> structure.createNode("eu-de", "region")).join();
        final Node cluster = structure.submit(() -> structure.createNode("eu-de-1", "cluster")).join();
        structure.run(() -> structure.contain(region.id(), cluster.id(), "placement")).join();
        structure.run(() -> structure.contain(region.id(), alpha.id(), "placement")).join();

        assertArrayEquals(new long[]{cluster.id(), alpha.id()},
                structure.children(region.id()).join());
        assertArrayEquals(new long[]{region.id()},
                structure.parents(alpha.id()).join());
        assertArrayEquals(new long[]{region.id()},
                structure.parents(cluster.id()).join());
    }
}
