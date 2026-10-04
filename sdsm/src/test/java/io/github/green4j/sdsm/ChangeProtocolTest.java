package io.github.green4j.sdsm;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A batch says what a value is, not just what it reads as, and carries the names of the keys it
 * mentions - so a receiver can act on it without holding the structure or its dictionary.
 */
class ChangeProtocolTest {
    private final StructureRuntime runtime = StructureRuntime.create(1, 1, "protocol");
    private final Structure structure = runtime.newStructure();

    private final Node alpha = structure.submit(() -> structure.createNode("Alpha", "pod")).join();
    private final Recorder recorder = new Recorder();

    @AfterEach
    void tearDown() {
        runtime.close();
    }

    private void subscribe() {
        subscribeTo("node");
    }

    private void subscribeTo(final String selectorText) {
        final View everything =
                structure.createView("everything", selectorText, null,
                        DeliveryPolicy.onChange()).join();
        structure.subscribe(everything.id(), recorder).join();
    }

    private void write(final Runnable writes) {
        structure.run(writes).join();
        structure.flushAll().join();
    }

    @Test
    void shouldCarryEachValueWithItsType() {
        subscribe();
        final int backlog = structure.propertyKeys().idOf("backlog");
        final int rate = structure.propertyKeys().idOf("rate");
        final int ready = structure.propertyKeys().idOf("ready");
        final int state = structure.propertyKeys().idOf("state");
        write(() -> {
            structure.setLong(alpha.id(), backlog, 84L);
            structure.setDouble(alpha.id(), rate, 1.5);
            structure.setBoolean(alpha.id(), ready, true);
            structure.setText(alpha.id(), state, "running");
        });

        assertEquals(ValueType.LONG, recorder.typeOf(alpha.id(), "backlog"));
        assertEquals("84", recorder.latestValueOf(alpha.id(), "backlog"));
        assertEquals(ValueType.DOUBLE, recorder.typeOf(alpha.id(), "rate"));
        assertEquals("1.5", recorder.latestValueOf(alpha.id(), "rate"));
        assertEquals(ValueType.BOOLEAN, recorder.typeOf(alpha.id(), "ready"));
        assertEquals("true", recorder.latestValueOf(alpha.id(), "ready"));
        assertEquals(ValueType.TEXT, recorder.typeOf(alpha.id(), "state"));
        assertEquals("running", recorder.latestValueOf(alpha.id(), "state"));
    }

    /**
     * A source says the whole of its area every round, so most of what it writes is what is
     * already there. A value written over itself is not a change: nobody is told, and the
     * structure's version does not move.
     */
    @Test
    void shouldNotDeliverAWriteThatChangesNothing() {
        final int backlog = structure.propertyKeys().idOf("backlog");
        subscribe();
        write(() -> structure.setLong(alpha.id(), backlog, 84L));
        final int afterFirst = recorder.recordsFor(alpha.id(), "backlog");

        write(() -> structure.setLong(alpha.id(), backlog, 84L));
        assertEquals(afterFirst, recorder.recordsFor(alpha.id(), "backlog"));

        write(() -> structure.setLong(alpha.id(), backlog, 85L));
        assertEquals(afterFirst + 1, recorder.recordsFor(alpha.id(), "backlog"));
    }

    @Test
    void shouldNotDeliverTheRemovalOfWhatIsNotThere() {
        final int backlog = structure.propertyKeys().idOf("backlog");
        subscribe();
        final int before = recorder.batches();

        write(() -> structure.removeProperty(alpha.id(), backlog));

        assertEquals(before, recorder.batches());
    }

    @Test
    void shouldReportARemovedPropertyAsAbsent() {
        final int backlog = structure.propertyKeys().idOf("backlog");
        Props.setLong(structure, alpha.id(), "backlog", 84L);
        subscribe();

        write(() -> structure.removeProperty(alpha.id(), backlog));

        assertEquals(ValueType.ABSENT, recorder.typeOf(alpha.id(), "backlog"));
    }

    @Test
    void shouldCarryTheIntrinsicPropertiesUnderTheirOwnKeys() {
        Props.setText(structure, alpha.id(), "state", "running");
        subscribe();

        assertEquals("Alpha", recorder.latestValueOf(alpha.id(), "$name"));
        assertEquals("pod", recorder.latestValueOf(alpha.id(), "$type"));
        assertEquals(Long.toString(alpha.id()), recorder.latestValueOf(alpha.id(), "$id"));
    }

    /**
     * A receiver holding no structure draws the graph out of the records alone, so which node
     * a port is on and which ports a link joins travel as properties of those objects.
     */
    @Test
    void shouldSayWhichNodeAPortIsOnAndWhichPortsALinkJoins() {
        final Node beta = structure.submit(() -> structure.createNode("Beta", "pod")).join();
        final Output from = structure.submit(() -> structure.addOutput(alpha.id(), "out", "tb")).join();
        final Input to = structure.submit(() -> structure.addInput(beta.id(), "in", "tb")).join();
        final Link edge =
                structure.submit(() -> structure.createLink("edge", "flow", from.id(), to.id())).join();
        subscribeTo("*");

        assertEquals(Long.toString(alpha.id()), recorder.latestValueOf(from.id(), "$node"));
        assertEquals(Long.toString(beta.id()), recorder.latestValueOf(to.id(), "$node"));
        assertEquals(Long.toString(from.id()), recorder.latestValueOf(edge.id(), "$from"));
        assertEquals(Long.toString(to.id()), recorder.latestValueOf(edge.id(), "$to"));
    }

    @Test
    void shouldSayWhichEndpointALinkWasRetargetedTo() {
        final Node beta = structure.submit(() -> structure.createNode("Beta", "pod")).join();
        final Output from = structure.submit(() -> structure.addOutput(alpha.id(), "out", "tb")).join();
        final Input to = structure.submit(() -> structure.addInput(beta.id(), "in", "tb")).join();
        final Input other = structure.submit(() -> structure.addInput(beta.id(), "spare", "tb")).join();
        final Link edge =
                structure.submit(() -> structure.createLink("edge", "flow", from.id(), to.id())).join();
        subscribeTo("*");

        write(() -> structure.retargetLinkTo(edge.id(), other.id()));

        assertEquals(Long.toString(other.id()), recorder.latestValueOf(edge.id(), "$to"));
    }

    /**
     * Membership is the one relation an object can have several of, and a record states one
     * pair: who joined what, and who left what.
     */
    @Test
    void shouldNameTheGroupAMemberJoinedAndTheOneItLeft() {
        final Node rack =
                structure.submit(() -> structure.createNode("rack-1", "rack")).join();
        structure.run(() -> structure.contain(rack.id(), alpha.id(), "placement")).join();
        subscribeTo("*");

        assertEquals(List.of(alpha.id() + " in " + rack.id()),
                recorder.membership(ChangeKind.CONTAINED));

        write(() -> structure.uncontain(rack.id(), alpha.id()));

        assertEquals(List.of(alpha.id() + " in " + rack.id()),
                recorder.membership(ChangeKind.UNCONTAINED));
    }

    /**
     * A snapshot says what is there before it says anything about it: a receiver holding
     * nothing cannot place a member in a group it has not been given yet, and which id sits
     * where in the structure's own index is nobody's contract.
     */
    @Test
    void shouldSayEverythingIsThereBeforeSayingWhatItIsIn() {
        structure.run(() -> {
            final Node region = structure.createNode("eu", "region");
            for (int i = 0; i < 16; i++) {
                final Node rack = structure.createNode("rack-" + i, "rack");
                structure.contain(region.id(), rack.id(), "placement");
                final Node pod = structure.createNode("pod-" + i, "pod");
                structure.contain(rack.id(), pod.id(), "placement");
            }
        }).join();
        subscribeTo("*");

        assertTrue(recorder.snapshotSaidWhatIsThereFirst());
        assertTrue(recorder.everyPairFollowsItsEnds());
        assertEquals(32, recorder.membership(ChangeKind.CONTAINED).size());
    }

    /**
     * A key id means one name everywhere, so an id taken from one structure is right for another.
     */
    @Test
    void shouldGiveAKeyOneIdInEveryStructure() {
        final int first = structure.propertyKeys().idOf("backlog");
        final Structure other = runtime.newStructure();

        assertEquals(first, structure.propertyKeys().idOf("backlog"));
        assertEquals(first, other.propertyKeys().idOf("backlog"));
        assertEquals("backlog", other.propertyKeys().nameOf(first));
    }
}
