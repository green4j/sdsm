package io.github.green4j.sdsm;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A group's sum of its members, worked out by the structure: over the summands it knows, with
 * how many it knows and how many there are, through every level of nesting at once.
 */
class DeriveTest {

    private final StructureRuntime runtime = StructureRuntime.create(1, 1, "derive");
    private final Structure structure = runtime.newStructure();
    private final int backlog = structure.propertyKeys().idOf("backlog");
    private final int rate = structure.propertyKeys().idOf("rate");

    private Node cluster;
    private Node blue;
    private Node green;
    private Node raw;
    private Node enriched;
    private Node cross;
    private Node aggregator;

    @BeforeEach
    void layOut() {
        cluster = structure.submit(() -> structure.createNode("eu-de-1", "cluster")).join();
        blue = structure.submit(() -> structure.createNode("blue", "release")).join();
        green = structure.submit(() -> structure.createNode("green", "release")).join();
        structure.run(() -> structure.contain(cluster.id(), blue.id(), "placement")).join();
        structure.run(() -> structure.contain(cluster.id(), green.id(), "placement")).join();
        raw = stream(blue, "raw");
        enriched = stream(blue, "enriched");
        cross = stream(green, "cross");
        aggregator = structure.submit(() -> structure.createNode("aggregator", "aggregate")).join();
        structure.run(() -> structure.contain(blue.id(), aggregator.id(), "placement")).join();
    }

    @AfterEach
    void tearDown() {
        runtime.close();
    }

    @Test
    void shouldSumTheMembersOfTheGivenTypeOnly() {
        set(raw, backlog, 100L);
        set(enriched, backlog, 20L);
        set(aggregator, backlog, 5000L);
        derive(blue, backlog);

        assertEquals(120L, value(blue, "backlog"));
        assertEquals(2L, value(blue, "backlog.known"));
        assertEquals(2L, value(blue, "backlog.total"));
    }

    /**
     * How many summands a child knows is a property like any other, and a parent folding it
     * follows it from the moment the child starts counting.
     */
    @Test
    void shouldFoldTheCountsOfChildren() {
        final int known = structure.propertyKeys().idOf("backlog.known");
        final int total = structure.propertyKeys().idOf("backlog.total");
        final int reporting = structure.propertyKeys().idOf("reporting");
        final int streams = structure.propertyKeys().idOf("streams");
        structure.run(() -> {
            structure.derive(cluster.id(), reporting, Fold.SUM, Over.children(known, "release"));
            structure.derive(cluster.id(), streams, Fold.SUM, Over.children(total, "release"));
        }).join();
        derive(blue, backlog);
        assertEquals(0L, value(cluster, "reporting"));
        assertEquals(2L, value(cluster, "streams"));

        set(raw, backlog, 100L);
        set(enriched, backlog, 20L);
        stream(blue, "late");

        assertEquals(2L, value(cluster, "reporting"));
        assertEquals(3L, value(cluster, "streams"));
    }

    /**
     * The sum is over what is known, and says how much that is. Nothing known is not zero: a
     * group whose summands nobody supplies has no sum.
     */
    @Test
    void shouldFollowEveryWriteOfASummand() {
        derive(blue, backlog);
        assertFalse(properties(blue).containsKey("backlog"));
        assertEquals(0L, value(blue, "backlog.known"));
        assertEquals(2L, value(blue, "backlog.total"));

        set(raw, backlog, 100L);
        set(enriched, backlog, 20L);
        set(raw, backlog, 70L);
        assertEquals(90L, value(blue, "backlog"));

        structure.run(() -> structure.removeProperty(enriched.id(), backlog)).join();
        assertEquals(70L, value(blue, "backlog"));
        assertEquals(1L, value(blue, "backlog.known"));
        assertEquals(2L, value(blue, "backlog.total"));

        structure.run(() -> structure.removeProperty(raw.id(), backlog)).join();
        assertFalse(properties(blue).containsKey("backlog"));
        assertEquals(0L, value(blue, "backlog.known"));
    }

    /**
     * The cluster counts streams, not releases: a release is a summand only by what it sums, and
     * one removed takes what it summed with it.
     */
    @Test
    void shouldCarrySumsAndCountsUpThroughNesting() {
        derive(blue, backlog);
        derive(green, backlog);
        derive(cluster, backlog);
        set(raw, backlog, 100L);
        set(cross, backlog, 1L);

        assertEquals(101L, value(cluster, "backlog"));
        assertEquals(2L, value(cluster, "backlog.known"));
        assertEquals(3L, value(cluster, "backlog.total"));

        set(enriched, backlog, 20L);
        assertEquals(121L, value(cluster, "backlog"));
        assertEquals(3L, value(cluster, "backlog.known"));

        structure.run(() -> structure.remove(green.id())).join();
        assertEquals(120L, value(cluster, "backlog"));
        assertEquals(2L, value(cluster, "backlog.total"));
    }

    @Test
    void shouldRecountWhenMembershipChanges() {
        derive(blue, backlog);
        derive(cluster, backlog);
        set(raw, backlog, 100L);
        set(enriched, backlog, 20L);

        final Node joined = stream(blue, "joined");
        set(joined, backlog, 3L);
        assertEquals(123L, value(cluster, "backlog"));
        assertEquals(3L, value(cluster, "backlog.total"));

        structure.run(() -> structure.uncontain(blue.id(), raw.id())).join();
        assertEquals(23L, value(blue, "backlog"));
        assertEquals(2L, value(cluster, "backlog.total"));

        structure.run(() -> structure.remove(enriched.id())).join();
        assertEquals(3L, value(cluster, "backlog"));
        assertEquals(1L, value(cluster, "backlog.known"));
    }

    /**
     * A value written by hand before the property was derived is not what the fold says: once
     * derived, the property is the fold's, even while it has nothing to fold.
     */
    @Test
    void shouldLetGoOfAValueTheFoldDoesNotSay() {
        set(cluster, rate, 5L);
        structure.run(() -> structure.derive(cluster.id(), rate, Fold.SUM, Over.children(rate, "stream"))).join();

        assertFalse(properties(cluster).containsKey("rate"), properties(cluster).toString());
        assertEquals(0L, value(cluster, "rate.total"));
    }

    @Test
    void shouldGoFractionalWhenAnySummandIs() {
        derive(blue, rate);
        set(raw, rate, 2L);
        structure.run(() -> structure.setDouble(enriched.id(), rate, 0.5)).join();
        assertEquals(2.5, value(blue, "rate"));

        set(enriched, rate, 1L);
        assertEquals(3L, value(blue, "rate"));
    }

    /**
     * The worst of the members, through nesting: a maximum cannot take a member back out, so
     * the group recounts, and the cluster sees the release fall back when its worst recovers.
     */
    @Test
    void shouldCarryTheWorstUpAndLetItFallBack() {
        derive(blue, backlog, Fold.MAX);
        derive(green, backlog, Fold.MAX);
        derive(cluster, backlog, Fold.MAX);
        set(raw, backlog, 100L);
        set(enriched, backlog, 20L);
        set(cross, backlog, 50L);
        assertEquals(100L, value(cluster, "backlog"));
        assertEquals(3L, value(cluster, "backlog.known"));

        set(raw, backlog, 10L);
        assertEquals(20L, value(blue, "backlog"));
        assertEquals(50L, value(cluster, "backlog"));

        structure.run(() -> structure.setDouble(enriched.id(), backlog, 70.5)).join();
        assertEquals(70.5, value(cluster, "backlog"));
    }

    @Test
    void shouldTakeEveryMemberWhenNoTypeIsGiven() {
        set(raw, backlog, 100L);
        set(aggregator, backlog, 5000L);
        set(green, backlog, 7L);
        structure.run(() -> structure.derive(cluster.id(), backlog, Fold.MAX, Over.children(backlog, null))).join();
        structure.run(() -> structure.derive(blue.id(), backlog, Fold.MAX, Over.children(backlog, null))).join();

        assertEquals(5000L, value(blue, "backlog"));
        assertEquals(2L, value(blue, "backlog.known"));
        assertEquals(3L, value(blue, "backlog.total"));
        // green folds nothing, so it is a summand with its own value
        assertEquals(5000L, value(cluster, "backlog"));
        assertEquals(3L, value(cluster, "backlog.known"));
        assertEquals(4L, value(cluster, "backlog.total"));
    }

    @Test
    void shouldCarryTheLeastUp() {
        derive(blue, backlog, Fold.MIN);
        set(raw, backlog, 100L);
        set(enriched, backlog, 20L);
        assertEquals(20L, value(blue, "backlog"));

        structure.run(() -> structure.removeProperty(enriched.id(), backlog)).join();
        assertEquals(100L, value(blue, "backlog"));
    }

    /**
     * A subgroup summing what its parent takes the worst of is not one of the parent's
     * summands: the two do not mean the same thing.
     */
    @Test
    void shouldNotFoldASubgroupThatFoldsOtherwise() {
        derive(blue, backlog);
        derive(cluster, backlog, Fold.MAX);
        set(raw, backlog, 100L);
        set(enriched, backlog, 20L);

        assertEquals(120L, value(blue, "backlog"));
        assertFalse(properties(cluster).containsKey("backlog"));
    }

    @Test
    void shouldRefuseToWriteWhatIsDerived() {
        derive(blue, backlog);

        assertRejected(() -> set(blue, backlog, 1L));
        assertRejected(() -> set(blue, structure.propertyKeys().idOf("backlog.known"), 1L));
    }

    @Test
    void shouldRefuseToDeriveWhatASourceSupplies() {
        structure.run(7, () -> structure.setLong(blue.id(), backlog, 1L)).join();

        assertRejected(() -> derive(blue, backlog));
    }

    @Test
    void shouldTakeTheSameDeclarationTwiceAndRefuseAnother() {
        derive(blue, backlog);
        derive(blue, backlog);

        assertRejected(() -> structure.run(() -> structure.derive(
                blue.id(), backlog, Fold.SUM, Over.children(rate, "stream"))).join());
        assertRejected(() -> derive(blue, backlog, Fold.MAX));
    }

    /**
     * What is not a number is one summand among others: once it has gone, the sum is a number
     * again, and an infinity against the opposite one is not a number while both are there.
     */
    @Test
    void shouldComeBackFromWhatIsNotANumber() {
        derive(blue, rate);
        derive(cluster, rate);

        set(raw, rate, Double.NaN);
        set(enriched, rate, 2.0);
        assertEquals(Double.NaN, value(blue, "rate"));
        set(raw, rate, Double.POSITIVE_INFINITY);
        assertEquals(Double.POSITIVE_INFINITY, value(blue, "rate"));
        set(enriched, rate, Double.NEGATIVE_INFINITY);
        assertEquals(Double.NaN, value(blue, "rate"));
        set(raw, rate, 1.0);
        assertEquals(Double.NEGATIVE_INFINITY, value(blue, "rate"));
        set(enriched, rate, 2.0);

        assertEquals(3.0, value(blue, "rate"));
        assertEquals(3.0, value(cluster, "rate"));
    }

    /**
     * A sum kept by adding and taking away does not let a large summand that has gone take a
     * small one with it.
     */
    @Test
    void shouldKeepASmallSummandALargeOneHasPassed() {
        derive(blue, rate);
        derive(cluster, rate);

        set(enriched, rate, 0.2);
        set(raw, rate, 1e17);
        set(raw, rate, 0.1);

        assertEquals(0.3, (double) value(blue, "rate"), 1e-15);
        assertEquals(0.3, (double) value(cluster, "rate"), 1e-15);
    }

    @Test
    void shouldComeBackFromASumTooLargeToHold() {
        derive(blue, rate);

        set(raw, rate, Double.MAX_VALUE);
        set(enriched, rate, Double.MAX_VALUE);
        assertEquals(Double.POSITIVE_INFINITY, value(blue, "rate"));
        set(raw, rate, -1.0);

        assertEquals(Double.MAX_VALUE, value(blue, "rate"));
    }

    /**
     * However long a sum is kept by adding and taking away, it says what the summands it holds
     * come to, to within the rounding of the last few writes.
     */
    @Test
    void shouldHoldASumThroughALongRunOfWrites() {
        derive(blue, rate);
        derive(green, rate);
        derive(cluster, rate);
        final Node[] streams = {raw, enriched, cross};
        final double[] last = new double[streams.length];
        final Random random = new Random(42L);
        structure.run(() -> {
            for (int i = 0; i < 100_000; i++) {
                final int at = random.nextInt(streams.length);
                last[at] = (random.nextDouble() - 0.5) * Math.pow(10.0, random.nextInt(18) - 3);
                structure.setDouble(streams[at].id(), rate, last[at]);
            }
        }).join();

        BigDecimal exact = BigDecimal.ZERO;
        double magnitude = 0.0;
        for (final double summand : last) {
            exact = exact.add(new BigDecimal(summand));
            magnitude += Math.abs(summand);
        }
        assertEquals(exact.doubleValue(), (double) value(cluster, "rate"), magnitude * 1e-15);
    }

    /**
     * A child coming or going costs its own summand, not a recount of its siblings.
     */
    @Test
    void shouldPayForAChildWhatItBrings() {
        derive(green, backlog);
        derive(cluster, backlog);
        final int children = 20_000;

        final long started = System.nanoTime();
        final long[] ids = structure.submit(() -> {
            final long[] made = new long[children];
            for (int i = 0; i < children; i++) {
                final Node child = structure.createNode("green/" + i, "stream");
                structure.setLong(child.id(), backlog, 1L);
                structure.contain(green.id(), child.id(), "placement");
                made[i] = child.id();
            }
            return made;
        }).join();
        structure.run(() -> {
            for (int i = 0; i < children / 2; i++) {
                structure.uncontain(green.id(), ids[i]);
            }
            for (int i = children / 2; i < children * 3 / 4; i++) {
                structure.remove(ids[i]);
            }
        }).join();
        final long elapsed = System.nanoTime() - started;

        assertEquals((long) children / 4, value(cluster, "backlog"));
        assertEquals((long) children / 4 + 1, value(green, "backlog.total"));
        assertTrue(elapsed < TimeUnit.MILLISECONDS.toNanos(1_000L), elapsed / 1_000_000 + " ms");
    }

    /**
     * A child that starts deriving, or whose derived value changes, costs its parent what the
     * child's value did, however many siblings it has.
     */
    @Test
    void shouldPayForADerivedChildWhatItChanged() {
        final int own = structure.propertyKeys().idOf("own");
        derive(green, rate);
        final int children = 20_000;

        final long started = System.nanoTime();
        structure.run(() -> {
            final long[] made = new long[children];
            for (int i = 0; i < children; i++) {
                final Node child = structure.createNode("green/" + i, "stream");
                structure.contain(green.id(), child.id(), "placement");
                structure.derive(child.id(), rate, Fold.SUM, Over.keys(own));
                made[i] = child.id();
            }
            for (int i = 0; i < children; i++) {
                structure.setLong(made[i], own, 2L);
            }
        }).join();
        final long elapsed = System.nanoTime() - started;

        assertEquals(2L * children, value(green, "rate"));
        assertEquals((long) children, value(green, "rate.known"));
        assertTrue(elapsed < TimeUnit.MILLISECONDS.toNanos(1_000L), elapsed / 1_000_000 + " ms");
    }

    private Node stream(final Node release, final String name) {
        final Node node = structure.submit(() -> structure.createNode(release.name() + "/" + name, "stream")).join();
        structure.run(() -> structure.contain(release.id(), node.id(), "placement")).join();
        return node;
    }

    private void derive(final Node group, final int keyId) {
        derive(group, keyId, Fold.SUM);
    }

    private void derive(final Node group, final int keyId, final Fold fold) {
        structure.run(() -> structure.derive(group.id(), keyId, fold, Over.children(keyId, "stream"))).join();
    }

    private void set(final StructureObject object, final int keyId, final long value) {
        structure.run(() -> structure.setLong(object.id(), keyId, value)).join();
    }

    private void set(final StructureObject object, final int keyId, final double value) {
        structure.run(() -> structure.setDouble(object.id(), keyId, value)).join();
    }

    private Map<String, Object> properties(final StructureObject object) {
        return structure.snapshotObject(object.id()).join();
    }

    private Object value(final StructureObject object, final String key) {
        final Map<String, Object> properties = properties(object);
        assertTrue(properties.containsKey(key), key + " missing from " + properties);
        return properties.get(key);
    }

    private static void assertRejected(final Runnable call) {
        final Throwable cause = assertThrows(CompletionException.class, call::run).getCause();
        assertTrue(cause instanceof IllegalArgumentException, String.valueOf(cause));
    }
}
