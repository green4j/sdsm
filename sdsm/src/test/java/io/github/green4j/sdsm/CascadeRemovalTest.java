package io.github.green4j.sdsm;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Removal cascades so that the structure cannot be left holding a link to a port which is gone.
 */
class CascadeRemovalTest {
    private final StructureRuntime runtime = StructureRuntime.create(1, 1, "cascade");
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

    private boolean exists(final long objectId) {
        return structure.matchedObjectIds("*[$id=" + objectId + "]").join().length == 1;
    }

    @ParameterizedTest(name = "the writer: {0}")
    @ValueSource(booleans = {true, false})
    void shouldRemoveThePortsAndTheLinksOfARemovedNode(final boolean writer) {
        final Node removed = writer ? alpha : beta;
        final Node other = writer ? beta : alpha;
        structure.run(() -> structure.remove(removed.id())).join();

        assertFalse(exists(removed.id()), "the node");
        assertFalse(exists((writer ? out : in).id()), "its port");
        assertFalse(exists(edge.id()), "the link which touched that port");
        assertTrue(exists(other.id()), "the node at the other end stays");
        assertTrue(exists((writer ? in : out).id()), "and so does its port");

        structure.run(() -> structure.remove(other.id())).join();
        assertFalse(exists((writer ? in : out).id()), "a port with no links goes as well");
    }

    @Test
    void shouldRemoveTheLinksOfARemovedPort() {
        structure.run(() -> structure.remove(in.id())).join();

        assertFalse(exists(in.id()));
        assertFalse(exists(edge.id()));
        assertTrue(exists(out.id()), "the port at the other end is not a dependency of the link");
    }

    @Test
    void shouldKeepTheMembersOfARemovedGroup() {
        final Node region = structure.submit(() -> structure.createNode("eu-de", "region")).join();
        structure.run(() -> structure.contain(region.id(), alpha.id(), "placement")).join();
        structure.run(() -> structure.remove(region.id())).join();

        assertFalse(exists(region.id()));
        assertTrue(exists(alpha.id()), "a group owns membership, not its members");
    }
}
