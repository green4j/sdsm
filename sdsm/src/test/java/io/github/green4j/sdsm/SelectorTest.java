package io.github.green4j.sdsm;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A selector is read once for what it depends on, so a write it cannot notice costs it
 * nothing; and a literal is compared as what it is written as.
 */
class SelectorTest {

    private final StructureRuntime runtime = StructureRuntime.create(1, 1, "selector");
    private final Structure structure = runtime.newStructure();
    private final PropertyKeys keys = structure.propertyKeys();

    @AfterEach
    void tearDown() {
        runtime.close();
    }

    @Test
    void shouldKnowWhichKeysItReads() {
        final Selector.Expression selector = parse("node[status=UP] & ([cpu=1], link[rate=2])");

        assertTrue(selector.reads(keys.idOf("status")));
        assertTrue(selector.reads(keys.idOf("cpu")));
        assertTrue(selector.reads(keys.idOf("rate")));
        assertFalse(selector.reads(keys.idOf("replicas")));
        assertFalse(parse("").reads(keys.idOf("status")));
    }

    @Test
    void shouldReadWhatTheEndsOfALinkAreSelectedBy() {
        final Selector.Expression selector = parse("touching(node[tier=shop])");

        assertTrue(selector.reads(keys.idOf("tier")));
        assertTrue(selector.readsEnds());
        assertTrue(selector.mayMatch(ObjectKind.LINK));
        assertFalse(selector.mayMatch(ObjectKind.NODE));
    }

    @Test
    void shouldReadWhatTheNodeOfAPortIsSelectedBy() {
        final Selector.Expression selector = parse("on(node[tier=shop])");

        assertTrue(selector.reads(keys.idOf("tier")));
        assertTrue(selector.readsOwner());
        assertFalse(selector.readsEnds());
        assertTrue(selector.mayMatch(ObjectKind.INPUT));
        assertTrue(selector.mayMatch(ObjectKind.OUTPUT));
        assertFalse(selector.mayMatch(ObjectKind.NODE));
    }

    /**
     * What it does not hold changes as what it negates does, so it reads the same.
     */
    @Test
    void shouldReadWhatItNegates() {
        final Selector.Expression selector = parse("node & !node[status=UP]");
        assertTrue(selector.reads(keys.idOf("status")));
        assertTrue(selector.mayMatch(ObjectKind.NODE));
        assertFalse(selector.mayMatch(ObjectKind.LINK));

        assertTrue(parse("!touching(node[tier=shop])").readsEnds());
        assertTrue(parse("!touching(node[tier=shop])").mayMatch(ObjectKind.NODE));
        assertTrue(parse("!on(node)").readsOwner());
        assertTrue(parse("!under(placement, /eu)").readsPlacement());
        assertTrue(parse("[status]").reads(keys.idOf("status")));
    }

    @Test
    void shouldKnowWhichKindsCanMatch() {
        final Selector.Expression nodes = parse("node[status=UP]");
        assertTrue(nodes.mayMatch(ObjectKind.NODE));
        assertFalse(nodes.mayMatch(ObjectKind.LINK));
        assertFalse(nodes.mayMatch(ObjectKind.INPUT));

        final Selector.Expression either = parse("node, input");
        assertTrue(either.mayMatch(ObjectKind.INPUT));
        assertFalse(either.mayMatch(ObjectKind.OUTPUT));

        assertFalse(parse("node & link").mayMatch(ObjectKind.NODE));
        assertTrue(parse("[status=UP]").mayMatch(ObjectKind.OUTPUT));
    }

    /**
     * A value is compared as what it is: a whole number is not a fraction and is exact however
     * large, a zero is a zero whatever its sign, text that looks like a number is text, and what
     * an ordering cannot compare - text, or nothing at all - it does not hold.
     *
     * @param type     what the value is written as
     * @param value    the value of {@code x} on the one node there is
     * @param selector what is asked
     * @param matches  whether it holds the node
     */
    @ParameterizedTest(name = "{0} {1} {2}: {3}")
    @CsvSource(delimiter = '|', value = {
        "LONG   | 1                | node[x=1]                   | true",
        "LONG   | 1                | node[x=1.5]                 | false",
        "LONG   | 1                | node[x=1.0]                 | true",
        "LONG   | 1                | node[x!=1.5]                | true",
        "LONG   | 1                | node[x=1d]                  | false",
        "LONG   | 9007199254740993 | node[x=9007199254740993]    | true",
        "LONG   | 9007199254740993 | node[x=9007199254740992]    | false",
        "LONG   | 80               | node[x>80]                  | false",
        "LONG   | 80               | node[x<=79]                 | false",
        "LONG   | 80               | node[x>=80]                 | true",
        "LONG   | 80               | node[x<80]                  | false",
        "LONG   | 80               | node[x<=80]                 | true",
        "LONG   | 80               | node[x>79.5]                | true",
        "LONG   | 80               | node[x<80.5]                | true",
        "DOUBLE | 0.75             | node[x<1]                   | true",
        "DOUBLE | 0.75             | node[x>=0.8]                | false",
        "DOUBLE | -0.0             | node[x=0]                   | true",
        "DOUBLE | -0.0             | node[x<0]                   | false",
        "DOUBLE | -0.0             | node[x>=0]                  | true",
        "DOUBLE | -0.0             | node[x>-0.0]                | false",
        "DOUBLE | -0.0             | node[x<=-0.0]               | true",
        "TEXT   | UP               | node[x>0]                   | false",
        "TEXT   | UP               | node[y>0]                   | false",
        "TEXT   | UP               | node[x]                     | true",
        "TEXT   | UP               | node[y]                     | false",
        "TEXT   | UP               | node & !*[y]                | true",
        "TEXT   | UP               | node[y!=1]                  | false",
        "TEXT   | UP               | node & ![y=1]               | true",
        "TEXT   | UP               | node & ![x=UP]              | false",
        "TEXT   | UP               | node & !(*[x=DOWN], [y])    | true",
        "TEXT   | UP               | !node                       | false",
        "TEXT   | UP               | !!node                      | true",
    })
    void shouldCompareAValueAsWhatItIs(final ValueType type,
                                       final String value,
                                       final String selector,
                                       final boolean matches) {
        final int x = keys.idOf("x");
        structure.run(() -> {
            final long node = structure.createNode("n", "service").id();
            switch (type) {
                case LONG:
                    structure.setLong(node, x, Long.parseLong(value));
                    break;
                case DOUBLE:
                    structure.setDouble(node, x, Double.parseDouble(value));
                    break;
                default:
                    structure.setText(node, x, value);
                    break;
            }
        }).join();

        assertEquals(matches ? 1 : 0, structure.matchedObjectIds(selector).join().length);
    }

    @ParameterizedTest
    @ValueSource(strings = {"node[cpu>high]", "node[", "under(placement)", "under(placement, /)",
        "!", "node & !", "node!"})
    void shouldRefuseWhatIsNotASelector(final String text) {
        assertThrows(IllegalArgumentException.class, () -> parse(text));
    }

    private Selector.Expression parse(final String text) {
        return Selector.parse(text, keys, structure::parentOn);
    }
}
