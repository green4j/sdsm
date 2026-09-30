package io.github.green4j.sdsm;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The engine looks everything up by id, so these stand where a {@code Map<Long, ?>} would - and
 * they are hand-written, tombstoned and compacted on removal. They are checked against the
 * collections they replace.
 */
class PrimitiveIndexTest {
    private static final int ROUNDS = 20_000;

    @Test
    void shouldAnswerLikeAMapOfBoxedIds() {
        final LongObjectMap<String> subject = new LongObjectMap<>();
        final Map<Long, String> oracle = new HashMap<>();
        final Random random = new Random(20260919L);

        for (int i = 0; i < ROUNDS; i++) {
            final long key = random.nextLong() % 512L;
            if (random.nextInt(3) == 0) {
                assertEquals(oracle.remove(key), subject.remove(key), "remove " + key);
            } else {
                final String value = "v" + i;
                oracle.put(key, value);
                subject.put(key, value);
            }
            assertEquals(oracle.size(), subject.size());
        }
        for (final Map.Entry<Long, String> entry : oracle.entrySet()) {
            assertEquals(entry.getValue(), subject.get(entry.getKey().longValue()));
        }
        assertNull(subject.get(Long.MIN_VALUE));
    }

    @Test
    void shouldAnswerLikeASetOfBoxedIds() {
        final LongSet subject = new LongSet();
        final Set<Long> oracle = new HashSet<>();
        final Random random = new Random(4242L);

        for (int i = 0; i < ROUNDS; i++) {
            final long value = random.nextLong() % 512L;
            if (random.nextInt(3) == 0) {
                assertEquals(oracle.remove(value), subject.remove(value), "remove " + value);
            } else {
                assertEquals(oracle.add(value), subject.add(value), "add " + value);
            }
            assertEquals(oracle.size(), subject.size());
        }
        for (int i = 0; i < subject.size(); i++) {
            assertTrue(oracle.contains(Long.valueOf(subject.valueAt(i))));
        }
        for (final Long value : oracle) {
            assertTrue(subject.contains(value.longValue()));
        }
    }

    @Test
    void shouldHoldEachChangedPropertyOnceInTheOrderItFirstChanged() {
        final DirtyProperties subject = new DirtyProperties();

        assertTrue(subject.mark(7L, 1));
        assertTrue(subject.mark(7L, 2));
        assertTrue(subject.mark(9L, 1));
        assertFalse(subject.mark(7L, 1), "already owed");

        assertEquals(3, subject.size());
        assertEquals(7L, subject.objectIdAt(0));
        assertEquals(1, subject.keyIdAt(0));
        assertEquals(9L, subject.objectIdAt(2));

        subject.clear();
        assertEquals(0, subject.size());
        assertTrue(subject.mark(7L, 1), "cleared");
    }

    /**
     * A view clears what it owed on every delivery: once a burst has grown the table, clearing a
     * single property still costs one.
     */
    @Test
    void shouldClearAtTheCostOfWhatIsHeldAfterABurst() {
        final DirtyProperties subject = new DirtyProperties();
        for (int i = 0; i < 1_000_000; i++) {
            subject.mark(i, 1);
        }
        subject.clear();

        final long started = System.nanoTime();
        for (int i = 0; i < 10_000; i++) {
            assertTrue(subject.mark(i, 2));
            subject.clear();
        }
        final long elapsed = System.nanoTime() - started;

        assertTrue(elapsed < TimeUnit.MILLISECONDS.toNanos(100L), elapsed / 1_000_000 + " ms");
        assertTrue(subject.mark(5L, 2), "cleared");
        assertFalse(subject.mark(5L, 2), "held");
    }

    /**
     * A feed clears what it coalesced on every batch: once a first load has grown the table,
     * clearing a single id still costs one.
     */
    @Test
    void shouldClearTextAtTheCostOfWhatIsHeldAfterABurst() {
        final TextObjectMap<String> subject = new TextObjectMap<>();
        for (int i = 0; i < 1_000_000; i++) {
            subject.put("pod-" + i, "v");
        }
        subject.clear();

        final long started = System.nanoTime();
        for (int i = 0; i < 10_000; i++) {
            subject.put("pod-1", "v");
            subject.clear();
        }
        final long elapsed = System.nanoTime() - started;

        assertTrue(elapsed < TimeUnit.MILLISECONDS.toNanos(100L), elapsed / 1_000_000 + " ms");
        assertNull(subject.get("pod-1"), "cleared");
    }

    @Test
    void shouldClearTextAfterRemovalsAndStayUsable() {
        final TextObjectMap<String> subject = new TextObjectMap<>();
        for (int round = 0; round < 1_000; round++) {
            for (int i = 0; i < 40; i++) {
                subject.put("k" + (round * 40 + i), "v");
            }
            for (int i = 0; i < 20; i++) {
                subject.remove("k" + (round * 40 + i));
            }
            subject.clear();
            assertEquals(0, subject.size());
        }
        subject.put("k", "v");
        assertEquals("v", subject.get("k"));
        assertNull(subject.get("k1"));
    }
}
