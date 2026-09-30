package io.github.green4j.sdsm;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A set cut by a key has state of its own: the flow links between the same two nodes carrying
 * the same family are one bundle, a node the structure keeps with the sum of what their writers
 * send, and the bundle follows its links as they come, go, change family and change rate.
 */
class GroupingTest {

    private final StructureRuntime runtime = StructureRuntime.create(1, 1, "grouping");
    private final Structure structure = runtime.newStructure();
    private final int rate = structure.propertyKeys().idOf("rate");
    private final int family = structure.propertyKeys().idOf("family");
    private final int stage = structure.propertyKeys().idOf("stage");

    private Node aggregator;
    private Node query;
    private Output enriched;
    private Output minutes;
    private Output hours;
    private Input minutesIn;
    private final Map<Output, Input> readerOf = new HashMap<>();
    private Grouping bundles;

    @BeforeEach
    void layOut() {
        aggregator = structure.submit(() -> structure.createNode("aggregator", "aggregator")).join();
        query = structure.submit(() -> structure.createNode("todayquery", "todayquery")).join();
        enriched = flow("enriched", "enriched", 100L);
        minutes = flow("bars-1m", "bars", 10L);
        minutesIn = readerOf.get(minutes);
        hours = flow("bars-1h", "bars", 1L);
        bundles = structure.submit(() -> structure.groupBy("bundles", "bundle", "link[type=flow]",
                "from.node", "to.node", "from.family")).join();
        structure.run(() -> structure.deriveEach(bundles.id(), rate, Fold.SUM, Over.members("from.rate"))).join();
    }

    @AfterEach
    void tearDown() {
        runtime.close();
    }

    @Test
    void shouldKeepABundleForEveryPairAndFamily() {
        assertEquals(2, structure.groups(bundles.id()).join().length);
        final Map<String, Object> bars = bundle("bars");

        assertEquals(11L, bars.get("rate"));
        assertEquals(2L, bars.get("rate.total"));
        assertEquals(aggregator.id(), bars.get("from.node"));
        assertEquals(query.id(), bars.get("to.node"));
        assertEquals(100L, bundle("enriched").get("rate"));
        assertEquals(2, structure.matchedObjectIds("node[type=bundle]").join().length);
    }

    @Test
    void shouldFoldBothEndsSideBySide() {
        final int received = structure.propertyKeys().idOf("received");
        structure.run(() -> structure.deriveEach(bundles.id(), received, Fold.SUM, Over.members("to.rate"))).join();
        set(minutesIn, rate, 7L);

        final Map<String, Object> bars = bundle("bars");
        assertEquals(11L, bars.get("rate"));
        assertEquals(7L, bars.get("received"));
        assertEquals(1L, bars.get("received.known"), "one reader has not said");
    }

    @Test
    void shouldFollowWhatItsLinksCarry() {
        set(minutes, rate, 30L);
        assertEquals(31L, bundle("bars").get("rate"));

        structure.run(() -> structure.setText(hours.id(), family, "enriched")).join();
        assertEquals(30L, bundle("bars").get("rate"));
        assertEquals(101L, bundle("enriched").get("rate"));

        structure.run(() -> structure.remove(minutes.id())).join();
        structure.run(() -> structure.remove(hours.id())).join();
        assertEquals(1, structure.groups(bundles.id()).join().length, "the last link took its bundle");
    }

    @Test
    void shouldCutNodesAsWellAndRefuseWhatIsNoPath() {
        structure.run(() -> {
            structure.setText(aggregator.id(), stage, "aggregate");
            structure.setText(query.id(), stage, "serve");
        }).join();
        final Grouping bands = structure.submit(() -> structure.groupBy("bands", "band",
                "node[stage=aggregate], node[stage=serve]", "stage")).join();

        assertEquals(2, structure.groups(bands.id()).join().length);
        assertEquals(1, structure.matchedObjectIds("node[type=band][stage=serve]").join().length);
        structure.run(() -> structure.removeGrouping(bands.id())).join();
        assertEquals(0, structure.matchedObjectIds("node[type=band]").join().length);

        assertThrows(CompletionException.class,
                () -> structure.submit(() -> structure.groupBy("bad", "x", "link", "from.node.stage")).join());
        final Throwable cause = assertThrows(CompletionException.class, () -> structure.run(
                () -> structure.derive(aggregator.id(), rate, Fold.SUM, Over.members("from.rate"))).join()).getCause();
        assertTrue(cause instanceof IllegalArgumentException, String.valueOf(cause));
    }

    /**
     * A group is known by {@code <grouping>:<key>}, so while a grouping lives the ids under its
     * name are its own: no second grouping takes the name, and no object takes such an id.
     */
    @Test
    void shouldKeepTheIdsUnderAGroupingsNameToItsGroups() {
        final long bars = bundleId("bars");
        final String barsId = structure.submit(
                () -> structure.lookupOrNull(bars).externalId()).join();

        refused(() -> structure.groupBy("bundles", "other", "link", "from.node"));
        refused(() -> structure.groupBy("a:b", "other", "link", "from.node"));
        refused(() -> structure.createNode("clash", "x", "bundles:mine"));
        assertEquals(bars, structure.submit(() -> structure.findByExternalId(barsId).id()).join());

        structure.submit(() -> structure.createNode("early", "x", "later:1")).join();
        refused(() -> structure.groupBy("later", "other", "link", "from.node"));

        structure.run(() -> structure.removeGrouping(bundles.id())).join();
        structure.submit(() -> structure.createNode("free", "x", "bundles:mine")).join();
    }

    /**
     * A key is a value with its type: the number 5, the text "5" and the text with the quotes
     * in it are three groups, each saying its key as it is.
     */
    @Test
    void shouldCutANumberAndItsSpellingApart() {
        final Grouping bands = structure.submit(() -> {
            final Node number = structure.createNode("number", "member");
            final Node spelling = structure.createNode("spelling", "member");
            final Node quoted = structure.createNode("quoted", "member");
            structure.setLong(number.id(), stage, 5L);
            structure.setText(spelling.id(), stage, "5");
            structure.setText(quoted.id(), stage, "\"5\"");
            return structure.groupBy("bands", "band", "node[type=member]", "stage");
        }).join();

        final long[] groups = structure.groups(bands.id()).join();
        assertEquals(3, groups.length);
        final Map<String, Object> byId = new HashMap<>();
        for (final long group : groups) {
            final StructureObject made = structure.submit(() -> structure.lookupOrNull(group)).join();
            byId.put(made.externalId(), structure.snapshotObject(group).join().get("stage"));
        }
        assertEquals(5L, byId.get("bands:5"));
        assertEquals("5", byId.get("bands:\"5\""));
        assertEquals("\"5\"", byId.get("bands:\\\"5\\\""));
    }

    /**
     * A group's key is what its members share, and the structure says it: written by hand, the
     * group would say one key and hold members of another.
     */
    @Test
    void shouldRefuseToWriteAGroupsKeyByHand() {
        final long bars = bundleId("bars");
        final int fromFamily = structure.propertyKeys().idOf("from.family");
        final int label = structure.propertyKeys().idOf("label");

        refused(() -> {
            structure.setText(bars, fromFamily, "lie");
            return null;
        });
        refused(() -> {
            structure.removeProperty(bars, fromFamily);
            return null;
        });
        structure.run(() -> structure.setText(bars, label, "bars bundle")).join();
        assertEquals("bars", structure.snapshotObject(bars).join().get("from.family"));
        assertEquals("bars bundle", structure.snapshotObject(bars).join().get("label"));
    }

    /**
     * Nor is a group's key a fold of its members, whether declared for one group, for every
     * group there is, or for a grouping that has no group yet.
     */
    @Test
    void shouldRefuseToFoldIntoAGroupsKey() {
        final long bars = bundleId("bars");
        final int fromFamily = structure.propertyKeys().idOf("from.family");
        final Grouping bands = structure.submit(
                () -> structure.groupBy("bands", "band", "node[type=nothing]", "stage")).join();

        refused(() -> {
            structure.derive(bars, fromFamily, Fold.SUM, Over.members("from.rate"));
            return null;
        });
        refused(() -> {
            structure.deriveEach(bundles.id(), fromFamily, Fold.SUM, Over.members("from.rate"));
            return null;
        });
        refused(() -> {
            structure.deriveEach(bands.id(), stage, Fold.SUM, Over.members("rate"));
            return null;
        });
        assertEquals("bars", structure.snapshotObject(bars).join().get("from.family"));
    }

    private void refused(final java.util.function.Supplier<?> change) {
        final Throwable cause = assertThrows(CompletionException.class,
                () -> structure.submit(change).join()).getCause();
        assertTrue(cause instanceof IllegalArgumentException, String.valueOf(cause));
    }

    private long bundleId(final String fam) {
        for (final long id : structure.groups(bundles.id()).join()) {
            if (fam.equals(structure.snapshotObject(id).join().get("from.family"))) {
                return id;
            }
        }
        throw new AssertionError("no bundle of " + fam);
    }

    /**
     * A port read through its node - its key and what its group folds - follows the node.
     */
    @Test
    void shouldFollowTheNodeAPortIsOn() {
        final int region = structure.propertyKeys().idOf("region");
        final int weight = structure.propertyKeys().idOf("weight");
        final long broker = structure.submit(() -> {
            final Node node = structure.createNode("broker", "broker");
            structure.setText(node.id(), region, "eu");
            structure.addInput(node.id(), "in", "queue");
            return node.id();
        }).join();
        final Grouping queues = structure.submit(
                () -> structure.groupBy("queues", "queues", "input[type=queue]", "node.region")).join();
        structure.run(() -> structure.deriveEach(queues.id(), weight, Fold.SUM, Over.members("node.weight"))).join();

        structure.run(() -> structure.setLong(broker, weight, 3L)).join();
        structure.run(() -> structure.setText(broker, region, "us")).join();

        final long[] groups = structure.groups(queues.id()).join();
        assertEquals(1, groups.length);
        final Map<String, Object> group = structure.snapshotObject(groups[0]).join();
        assertEquals("us", group.get("node.region"));
        assertEquals(3L, group.get("weight"));
    }

    /**
     * A selector that reads placement holds a member placed below its path, however it got there:
     * put there itself, or with a parent put there.
     */
    @Test
    void shouldFollowAMemberAsItIsPlaced() {
        final long[] ids = structure.submit(() -> new long[]{
                structure.createNode("eu", "region").id(),
                structure.createNode("silo-1", "silo").id(),
                structure.createNode("pod-1", "pod").id()}).join();
        structure.run(() -> structure.setText(ids[2], stage, "serve")).join();
        final Grouping placed = structure.submit(() -> structure.groupBy("placed", "band",
                "node[type=pod] & under(placement, /eu)", "stage")).join();

        structure.run(() -> {
            structure.contain(ids[1], ids[2], "placement");
            structure.contain(ids[0], ids[1], "placement");
        }).join();
        assertEquals(1, structure.groups(placed.id()).join().length);

        structure.run(() -> structure.uncontain(ids[0], ids[1])).join();
        assertEquals(0, structure.groups(placed.id()).join().length);
    }

    /**
     * A member's write is folded into its group by what it changed, not by counting every member
     * again: a round of writes to a group of many costs what the writes cost.
     */
    @Test
    @Timeout(2)
    void shouldFoldAMembersWriteWithoutCountingTheOthersAgain() {
        final int members = 20_000;
        final long[] ids = new long[members];
        structure.run(() -> {
            for (int i = 0; i < members; i++) {
                ids[i] = structure.createNode("member-" + i, "member").id();
                structure.setText(ids[i], family, "herd");
            }
        }).join();
        final Grouping herds =
                structure.submit(() -> structure.groupBy("herds", "herd", "node[type=member]", "family")).join();
        structure.run(() -> structure.deriveEach(herds.id(), rate, Fold.SUM, Over.members("rate"))).join();

        structure.run(() -> {
            for (int i = 0; i < members; i++) {
                structure.setLong(ids[i], rate, i);
            }
        }).join();

        final long herd = structure.groups(herds.id()).join()[0];
        assertEquals((long) members * (members - 1) / 2, structure.snapshotObject(herd).join().get("rate"));
    }

    /**
     * A sum recounted because it could not take a summand back - it had overflowed - keeps what
     * each member brings, so the writes after it are folded by what they changed.
     */
    @Test
    void shouldKeepWhatEachMemberBringsThroughARecount() {
        final long[] ids = structure.submit(() -> {
            final long[] made = new long[2];
            for (int i = 0; i < made.length; i++) {
                made[i] = structure.createNode("member-" + i, "member").id();
                structure.setText(made[i], family, "herd");
                structure.setDouble(made[i], rate, Double.MAX_VALUE);
            }
            return made;
        }).join();
        final Grouping herds =
                structure.submit(() -> structure.groupBy("herds", "herd", "node[type=member]", "family")).join();
        structure.run(() -> structure.deriveEach(herds.id(), rate, Fold.SUM, Over.members("rate"))).join();
        final long herd = structure.groups(herds.id()).join()[0];

        for (final long id : ids) {
            structure.run(() -> structure.setDouble(id, rate, 1.0)).join();
            structure.run(() -> structure.setDouble(id, rate, Double.MAX_VALUE)).join();
            assertEquals(Double.POSITIVE_INFINITY, structure.snapshotObject(herd).join().get("rate"));
        }
        structure.run(() -> structure.setDouble(ids[0], rate, 1.0)).join();
        structure.run(() -> structure.setDouble(ids[0], rate, 2.0)).join();

        assertEquals(Double.MAX_VALUE, (double) structure.snapshotObject(herd).join().get("rate"), 0.0);
    }

    /**
     * A group is its grouping's, as a link drawn from addresses is the declarations': it goes
     * when its last member does, or with the grouping, and not on its own.
     */
    @Test
    void shouldRefuseToRemoveAGroupOnItsOwn() {
        final long group = structure.groups(bundles.id()).join()[0];

        final Throwable cause = assertThrows(CompletionException.class,
                () -> structure.run(() -> structure.remove(group)).join()).getCause();

        assertTrue(cause instanceof IllegalArgumentException, String.valueOf(cause));
        assertEquals(2, structure.groups(bundles.id()).join().length);
        assertEquals(11L, bundle("bars").get("rate"));
    }

    /**
     * A fold of every group that cannot be declared on one of them is declared on none, and
     * the groups to come are made as before.
     */
    @Test
    void shouldChangeNothingWhenAFoldOfEveryGroupIsRefused() {
        assertThrows(CompletionException.class, () -> structure.run(() -> structure.deriveEach(
                bundles.id(), rate, Fold.MAX, Over.members("from.rate"))).join());

        flow("bars-1d", "daily", 3L);

        assertEquals(3L, bundle("daily").get("rate"));
    }

    @Test
    void shouldCutByWhatHoldsThem() {
        final long blue = structure.submit(() -> structure.createNode("blue", "silo")).join().id();
        final long green = structure.submit(() -> structure.createNode("green", "silo")).join().id();
        final Node other = structure.submit(() -> structure.createNode("aggregator-1", "aggregator")).join();
        final int region = structure.propertyKeys().idOf("region");
        structure.run(() -> structure.contain(blue, aggregator.id(), "placement")).join();
        structure.run(() -> structure.contain(blue, query.id(), "placement")).join();
        structure.run(() -> structure.contain(green, other.id(), "placement")).join();
        structure.run(() -> {
            for (final Node node : new Node[]{aggregator, other}) {
                structure.setText(node.id(), stage, "aggregate");
            }
            structure.setText(query.id(), stage, "serve");
            structure.setText(blue, region, "eu-de");
            structure.setText(green, region, "eu-fr");
        }).join();
        final Grouping bands = structure.submit(() -> structure.groupBy("bands", "band",
                "node[stage=aggregate], node[stage=serve]", "parent(placement)", "stage")).join();
        final Grouping regions = structure.submit(() -> structure.groupBy("regions", "area", "node[stage=aggregate]",
                "parent(placement).region")).join();

        assertEquals(3, structure.groups(bands.id()).join().length);
        assertEquals(blue, band("serve").get("parent(placement)"));
        assertEquals(2, structure.groups(regions.id()).join().length);

        structure.run(() -> structure.uncontain(green, other.id())).join();
        assertEquals(2, structure.groups(bands.id()).join().length, "what is held by nothing is in no band");
        structure.run(() -> structure.contain(blue, other.id(), "placement")).join();
        assertEquals(2, structure.groups(bands.id()).join().length);
        assertEquals(1, structure.groups(regions.id()).join().length);

        structure.run(() -> structure.setText(blue, region, "eu-fr")).join();
        assertEquals(1, structure.matchedObjectIds("node[type=area][name=eu-fr]").join().length);
        assertThrows(CompletionException.class,
                () -> structure.submit(() -> structure.groupBy("bad", "x", "node", "parent()")).join());
    }

    private Output flow(final String name, final String kindOfFlow, final long sent) {
        final Output out = structure.submit(() -> structure.addOutput(aggregator.id(), name, "flow")).join();
        final Input in = structure.submit(() -> structure.addInput(query.id(), name, "flow")).join();
        readerOf.put(out, in);
        structure.run(() -> {
            structure.setText(out.id(), family, kindOfFlow);
            structure.setLong(out.id(), rate, sent);
            structure.provide(out.id(), name);
            structure.require(in.id(), name);
        }).join();
        return out;
    }

    private void set(final StructureObject object, final int keyId, final long value) {
        structure.run(() -> structure.setLong(object.id(), keyId, value)).join();
    }

    private Map<String, Object> band(final String ofStage) {
        final long[] ids = structure.matchedObjectIds("node[type=band][stage=" + ofStage + "]").join();
        assertEquals(1, ids.length, ofStage);
        return structure.snapshotObject(ids[0]).join();
    }

    private Map<String, Object> bundle(final String ofFamily) {
        final long[] ids = structure.matchedObjectIds("node[type=bundle][from.family=" + ofFamily + "]").join();
        assertEquals(1, ids.length, ofFamily);
        return structure.snapshotObject(ids[0]).join();
    }
}
