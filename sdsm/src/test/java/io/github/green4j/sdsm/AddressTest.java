package io.github.green4j.sdsm;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Nobody observes a connection. A pod's configuration says which queue it reads and which it
 * writes, and the two ends of one queue are seen apart, often out of order and sometimes with
 * a long wait between them. So each side declares an address instead, and the structure draws
 * the link when both are there.
 */
class AddressTest {
    private static final long NO_LINK = -1L;

    private final StructureRuntime runtime = StructureRuntime.create(1, 1, "address");
    private final Structure structure = runtime.newStructure();

    @AfterEach
    void tearDown() {
        runtime.close();
    }

    private static Throwable causeOf(final Executable call) {
        return assertThrows(CompletionException.class, call).getCause();
    }

    private long node(final String name) {
        return structure.submit(() -> structure.createNode(name, "pod")).join().id();
    }

    private long output(final long nodeId) {
        return structure.submit(() -> structure.addOutput(nodeId, "out", "tcp")).join().id();
    }

    private long input(final long nodeId) {
        return structure.submit(() -> structure.addInput(nodeId, "in", "tcp")).join().id();
    }

    private void provide(final long portId, final String address, final int domainId) {
        structure.run(() -> structure.provide(portId, address, domainId)).join();
    }

    private void require(final long portId, final String address, final int domainId) {
        structure.run(() -> structure.require(portId, address, domainId)).join();
    }

    private long[] linkIds() {
        return structure.matchedObjectIds("link").join();
    }

    private long linkBetween(final long fromOutputId, final long toInputId) {
        final long[] ids = linkIds();
        for (int i = 0; i < ids.length; i++) {
            final long[] ends = structure.linkEndpoints(ids[i]).join();
            if (ends[0] == fromOutputId && ends[2] == toInputId) {
                return ids[i];
            }
        }
        return NO_LINK;
    }

    @ParameterizedTest(name = "provider first: {0}")
    @ValueSource(booleans = {true, false})
    void shouldLinkTheTwoSidesInWhicheverOrderTheyCome(final boolean providerFirst) {
        final long producer = output(node("ingest"));
        final long consumer = input(node("aggregate"));

        if (providerFirst) {
            provide(producer, "tb:trades.eu", Domains.NO_DOMAIN);
        } else {
            require(consumer, "tb:trades.eu", Domains.NO_DOMAIN);
        }
        assertEquals(0, linkIds().length);
        if (providerFirst) {
            require(consumer, "tb:trades.eu", Domains.NO_DOMAIN);
        } else {
            provide(producer, "tb:trades.eu", Domains.NO_DOMAIN);
        }

        assertEquals(1, linkIds().length);
        assertNotEquals(NO_LINK, linkBetween(producer, consumer));
    }

    /**
     * A queue is a node of its own, because what goes in and what comes out are two rates and
     * a link carries one. Its two ports provide the same address, and the ends of the pipeline
     * attach to whichever of them faces them.
     */
    @Test
    void shouldJoinProducerAndConsumerThroughAMedium() {
        final long medium = node("trades");
        final long into = input(medium);
        final long outOf = output(medium);
        provide(into, "tb:trades.eu", Domains.NO_DOMAIN);
        provide(outOf, "tb:trades.eu", Domains.NO_DOMAIN);

        final long producer = output(node("ingest"));
        final long consumer = input(node("aggregate"));
        require(producer, "tb:trades.eu", Domains.NO_DOMAIN);
        require(consumer, "tb:trades.eu", Domains.NO_DOMAIN);

        assertEquals(2, linkIds().length);
        assertNotEquals(NO_LINK, linkBetween(producer, into));
        assertNotEquals(NO_LINK, linkBetween(outOf, consumer));
    }

    @Test
    void shouldNotLinkTwoSidesThatBothLookForTheAddress() {
        final long left = output(node("ingest"));
        final long right = input(node("aggregate"));

        require(left, "tb:trades.eu", Domains.NO_DOMAIN);
        require(right, "tb:trades.eu", Domains.NO_DOMAIN);

        assertEquals(0, linkIds().length);
    }

    @Test
    void shouldTakeTheLinkAwayWithTheNodeThatDeclaredIt() {
        final long ingest = node("ingest");
        final long producer = output(ingest);
        final long consumer = input(node("aggregate"));
        provide(producer, "tb:trades.eu", Domains.NO_DOMAIN);
        require(consumer, "tb:trades.eu", Domains.NO_DOMAIN);

        structure.run(() -> structure.remove(ingest)).join();

        assertEquals(0, linkIds().length);
    }

    @Test
    void shouldRedrawTheLinkWhenAPortIsGivenAnotherAddress() {
        final long producer = output(node("ingest"));
        final long consumer = input(node("aggregate"));
        provide(producer, "tb:trades.eu", Domains.NO_DOMAIN);
        require(consumer, "tb:trades.eu", Domains.NO_DOMAIN);

        require(consumer, "tb:aggr.eu", Domains.NO_DOMAIN);
        assertEquals(0, linkIds().length);

        provide(producer, "tb:aggr.eu", Domains.NO_DOMAIN);
        assertNotEquals(NO_LINK, linkBetween(producer, consumer));
    }

    @Test
    void shouldRefuseToRemoveOrRewireALinkItDrewItself() {
        final long producer = output(node("ingest"));
        final long consumer = input(node("aggregate"));
        provide(producer, "tb:trades.eu", Domains.NO_DOMAIN);
        require(consumer, "tb:trades.eu", Domains.NO_DOMAIN);
        final long drawn = linkBetween(producer, consumer);
        final long elsewhere = input(node("cross"));

        final Throwable removed = causeOf(() -> structure.run(() -> structure.remove(drawn)).join());
        final Throwable rewired =
                causeOf(() -> structure.run(() -> structure.retargetLinkTo(drawn, elsewhere)).join());

        assertTrue(removed instanceof IllegalArgumentException, String.valueOf(removed));
        assertTrue(rewired instanceof IllegalArgumentException, String.valueOf(rewired));
        assertEquals(1, linkIds().length);
        assertNotEquals(NO_LINK, linkBetween(producer, consumer));
    }

    /**
     * A subscriber hears of a drawn link the way it hears of any other. The declaration is a
     * property of a port and a view on links never sees it, so the link appearing is the only
     * word a client gets that the two sides have found each other.
     */
    @Test
    void shouldDeliverTheLinkItDrewLikeAnyOther() {
        final Recorder recorder = new Recorder();
        final View links = structure.createView("links", "link", null,
                DeliveryPolicy.onChange()).join();
        structure.subscribe(links.id(), recorder).join();

        final long producer = output(node("ingest"));
        final long consumer = input(node("aggregate"));
        provide(producer, "tb:trades.eu", Domains.NO_DOMAIN);
        require(consumer, "tb:trades.eu", Domains.NO_DOMAIN);
        structure.flushAll().join();
        final long drawn = linkBetween(producer, consumer);

        // a link comes with its two ports
        assertEquals(List.of(drawn, producer, consumer), recorder.idsAfterSnapshot(ChangeKind.ADDED));

        structure.run(() -> structure.clearAddress(producer)).join();
        structure.flushAll().join();

        assertEquals(List.of(drawn, producer, consumer), recorder.idsAfterSnapshot(ChangeKind.REMOVED));
    }

    @Test
    void shouldRefuseAnAddressOnAnythingButAPort() {
        final long ingest = node("ingest");

        final Throwable cause = causeOf(
                () -> structure.run(() -> structure.provide(ingest, "tb:trades.eu")).join());

        assertTrue(cause instanceof IllegalArgumentException, String.valueOf(cause));
    }

    /**
     * A declaration is properties of the port it is on, and a requirer nobody answers is what a
     * monitor looks for first: a selector finds it.
     */
    @Test
    void shouldSayWhoIsWaitingForAnAddressNobodyProvides() {
        final int blue = structure.domains().idOf("blue");
        final long consumer = input(node("aggregate"));
        require(consumer, "tb:trades.eu", blue);
        final String waiting = "input[role=require] & *[links=0]";

        final Map<String, Object> properties = structure.snapshotObject(consumer).join();
        assertEquals("tb:trades.eu", properties.get("address"));
        assertEquals("require", properties.get("role"));
        assertEquals("blue", properties.get("domain"));
        assertEquals(0L, properties.get("links"));
        assertEquals(1, structure.matchedObjectIds(waiting).join().length);
        assertEquals(1, structure.matchedObjectIds("input[address=\"tb:trades.eu\"]").join().length);

        final long producer = output(node("ingest"));
        provide(producer, "tb:trades.eu", blue);
        assertEquals(1L, structure.snapshotObject(consumer).join().get("links"));
        assertEquals(0, structure.matchedObjectIds(waiting).join().length);

        structure.run(() -> structure.clearAddress(producer)).join();
        assertEquals(1, structure.matchedObjectIds(waiting).join().length);
    }

    /**
     * Blue and green run the same pipeline and publish under the same names, so the address
     * alone cannot say which copy of the world is meant: they meet only by a route.
     */
    @Test
    void shouldKeepTheCopiesOfOneWorldApartUnlessRouted() {
        final int blue = structure.domains().idOf("blue");
        final int green = structure.domains().idOf("green");
        final long blueOut = output(node("aggregate-blue"));
        final long greenIn = input(node("cross-green"));
        provide(blueOut, "tb:aggr.eu", blue);
        require(greenIn, "tb:aggr.eu", green);
        assertEquals(0, linkIds().length);

        structure.run(() -> structure.allowRoute(green, blue)).join();
        assertNotEquals(NO_LINK, linkBetween(blueOut, greenIn));

        structure.run(() -> structure.denyRoute(green, blue)).join();
        assertEquals(0, linkIds().length);
    }

    /**
     * What belongs to no copy is reached from every one without a route, so a route to it
     * neither adds a link nor takes one away.
     */
    @Test
    void shouldNotRouteToWhatBelongsToNoCopy() {
        final int green = structure.domains().idOf("green");
        final long trades = output(node("trades"));
        final long greenIn = input(node("aggregate-green"));
        provide(trades, "tb:trades.eu", Domains.NO_DOMAIN);
        require(greenIn, "tb:trades.eu", green);

        structure.run(() -> structure.allowRoute(green, Domains.NO_DOMAIN)).join();
        assertEquals(1, linkIds().length);

        structure.run(() -> structure.denyRoute(green, Domains.NO_DOMAIN)).join();
        assertEquals(1, linkIds().length);
    }

    /**
     * The acceptance of the phase: one input TimeBase belongs to no colour, so both releases
     * read from it, while each writes to the copy of the output that carries its own.
     */
    @Test
    void shouldHandOneSharedInputToBothColoursAndKeepTheirOutputsApart() {
        final int blue = structure.domains().idOf("blue");
        final int green = structure.domains().idOf("green");

        final long trades = output(node("trades"));
        provide(trades, "tb:trades.eu", Domains.NO_DOMAIN);

        final long aggrBlue = input(node("aggr-blue"));
        final long aggrGreen = input(node("aggr-green"));
        provide(aggrBlue, "tb:aggr.eu", blue);
        provide(aggrGreen, "tb:aggr.eu", green);

        final long blueNode = node("aggregate-blue");
        final long greenNode = node("aggregate-green");
        final long blueIn = input(blueNode);
        final long blueOut = output(blueNode);
        final long greenIn = input(greenNode);
        final long greenOut = output(greenNode);
        require(blueIn, "tb:trades.eu", blue);
        require(greenIn, "tb:trades.eu", green);
        require(blueOut, "tb:aggr.eu", blue);
        require(greenOut, "tb:aggr.eu", green);

        assertEquals(4, linkIds().length);
        assertNotEquals(NO_LINK, linkBetween(trades, blueIn));
        assertNotEquals(NO_LINK, linkBetween(trades, greenIn));
        assertNotEquals(NO_LINK, linkBetween(blueOut, aggrBlue));
        assertNotEquals(NO_LINK, linkBetween(greenOut, aggrGreen));
        assertEquals(NO_LINK, linkBetween(blueOut, aggrGreen));
        assertEquals(NO_LINK, linkBetween(greenOut, aggrBlue));
    }
}
