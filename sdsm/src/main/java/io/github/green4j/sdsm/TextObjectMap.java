package io.github.green4j.sdsm;

import java.util.Arrays;

/**
 * A map from text to something, open-addressed, probed by any {@link CharSequence}.
 * A binding resolves an external id on every observation it takes; {@code Map<String, ?>}
 * would make it build a String for each lookup only to throw it away, so a probe here
 * compares characters and a reusable StringBuilder is enough to search with.
 * <p>
 * Keys are held as they were put in, so nothing is copied twice. A key must spell the same
 * text for as long as it is in the map.
 *
 * @param <V> what the text maps to
 */
final class TextObjectMap<V> {

    private static final int FREE = 0;
    private static final int TOMBSTONE = -1;

    private CharSequence[] keys = new CharSequence[16];
    private Object[] values = new Object[16];
    /** Where each key sits in the slot table, so clearing costs the keys held, not the table. */
    private int[] slotIndexes = new int[16];
    private int size;

    private int[] slots = new int[64];
    private int mask = slots.length - 1;
    private int occupied;

    int size() {
        return size;
    }

    CharSequence keyAt(final int index) {
        return keys[index];
    }

    @SuppressWarnings("unchecked")
    V valueAt(final int index) {
        return (V) values[index];
    }

    @SuppressWarnings("unchecked")
    V get(final CharSequence key) {
        final int slot = slotOf(key);
        return slot < 0 ? null : (V) values[slots[slot] - 1];
    }

    void put(final CharSequence key, final V value) {
        final int slot = slotOf(key);
        if (slot >= 0) {
            values[slots[slot] - 1] = value;
            return;
        }
        if (size == keys.length) {
            keys = Arrays.copyOf(keys, size * 2);
            values = Arrays.copyOf(values, size * 2);
            slotIndexes = Arrays.copyOf(slotIndexes, size * 2);
        }
        keys[size] = key;
        values[size] = value;
        size++;
        insertSlot(key, size);
        if (occupied * 4 > slots.length * 3) {
            rehash();
        }
    }

    void remove(final CharSequence key) {
        final int slot = slotOf(key);
        if (slot < 0) {
            return;
        }
        final int dense = slots[slot] - 1;
        slots[slot] = TOMBSTONE;
        final int last = size - 1;
        if (dense != last) {
            final CharSequence movedKey = keys[last];
            final int movedSlot = slotOf(movedKey);
            keys[dense] = movedKey;
            values[dense] = values[last];
            slotIndexes[dense] = movedSlot;
            slots[movedSlot] = dense + 1;
        }
        keys[last] = null;
        values[last] = null;
        size = last;
    }

    /**
     * Frees the slots of what is held rather than the whole table, which a burst has grown and
     * which does not shrink. What removals left behind stays counted as occupied, so a probe
     * still finds a free slot.
     */
    void clear() {
        for (int i = 0; i < size; i++) {
            slots[slotIndexes[i]] = FREE;
        }
        occupied -= size;
        Arrays.fill(keys, 0, size, null);
        Arrays.fill(values, 0, size, null);
        size = 0;
    }

    private int slotOf(final CharSequence key) {
        int index = hash(key) & mask;
        while (true) {
            final int entry = slots[index];
            if (entry == FREE) {
                return -1;
            }
            if (entry != TOMBSTONE && sameText(keys[entry - 1], key)) {
                return index;
            }
            index = (index + 1) & mask;
        }
    }

    private void insertSlot(final CharSequence key, final int denseIndexPlusOne) {
        int index = hash(key) & mask;
        while (slots[index] > 0) {
            index = (index + 1) & mask;
        }
        if (slots[index] == FREE) {
            occupied++;
        }
        slots[index] = denseIndexPlusOne;
        slotIndexes[denseIndexPlusOne - 1] = index;
    }

    private void rehash() {
        final int capacity = Math.max(64, Integer.highestOneBit(size * 4 - 1) * 2);
        slots = new int[capacity];
        mask = capacity - 1;
        occupied = 0;
        for (int i = 0; i < size; i++) {
            insertSlot(keys[i], i + 1);
        }
    }

    static boolean sameText(final CharSequence stored, final CharSequence probe) {
        if (stored instanceof String) {
            return probe instanceof String
                    ? stored.equals(probe)
                    : ((String) stored).contentEquals(probe);
        }
        final int length = stored.length();
        if (length != probe.length()) {
            return false;
        }
        for (int i = 0; i < length; i++) {
            if (stored.charAt(i) != probe.charAt(i)) {
                return false;
            }
        }
        return true;
    }

    /**
     * The polynomial String itself uses, so a String key and a StringBuilder probe
     * spelling the same text land in the same slot.
     *
     * @param key text to hash
     * @return its hash
     */
    private static int hash(final CharSequence key) {
        return spread(key instanceof String ? key.hashCode() : polynomialOf(key));
    }

    /**
     * @param key text a String would not have worked the polynomial out for
     * @return what {@link String#hashCode()} would have said
     */
    private static int polynomialOf(final CharSequence key) {
        int result = 0;
        for (int i = 0; i < key.length(); i++) {
            result = result * 31 + key.charAt(i);
        }
        return result;
    }

    private static int spread(final int polynomial) {
        final int mixed = polynomial * 0x9E3779B1;
        return mixed ^ (mixed >>> 16);
    }
}
