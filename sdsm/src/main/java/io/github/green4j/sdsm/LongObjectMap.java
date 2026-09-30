package io.github.green4j.sdsm;

import java.util.AbstractList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * A map from object id to something, open-addressed over primitive keys. Every lookup in the
 * engine goes through an id, and {@code Map<Long, ?>} would box it on each one.
 * <p>
 * Values sit in a dense array a caller can walk by index. A removal moves the last value into
 * the hole, so iteration order is not part of the contract.
 *
 * @param <V> what the ids map to
 */
final class LongObjectMap<V> {

    private static final int FREE = 0;
    private static final int TOMBSTONE = -1;

    private long[] keys = new long[16];
    private Object[] values = new Object[16];
    private int size;

    private int[] slots = new int[64];
    private int mask = slots.length - 1;
    private int occupied;

    private final List<V> valuesView = Collections.unmodifiableList(new AbstractList<V>() {
        @Override
        public V get(final int index) {
            return valueAt(index);
        }

        @Override
        public int size() {
            return size;
        }
    });

    int size() {
        return size;
    }

    @SuppressWarnings("unchecked")
    V valueAt(final int index) {
        return (V) values[index];
    }

    long keyAt(final int index) {
        return keys[index];
    }

    List<V> values() {
        return valuesView;
    }

    @SuppressWarnings("unchecked")
    V get(final long key) {
        final int slot = slotOf(key);
        return slot < 0 ? null : (V) values[slots[slot] - 1];
    }

    boolean containsKey(final long key) {
        return slotOf(key) >= 0;
    }

    void put(final long key, final V value) {
        final int slot = slotOf(key);
        if (slot >= 0) {
            values[slots[slot] - 1] = value;
            return;
        }
        if (size == keys.length) {
            keys = Arrays.copyOf(keys, size * 2);
            values = Arrays.copyOf(values, size * 2);
        }
        keys[size] = key;
        values[size] = value;
        size++;
        insertSlot(key, size);
        if (occupied * 4 > slots.length * 3) {
            rehash();
        }
    }

    @SuppressWarnings("unchecked")
    V remove(final long key) {
        final int slot = slotOf(key);
        if (slot < 0) {
            return null;
        }
        final int dense = slots[slot] - 1;
        final V removed = (V) values[dense];
        slots[slot] = TOMBSTONE;
        final int last = size - 1;
        if (dense != last) {
            final long movedKey = keys[last];
            keys[dense] = movedKey;
            values[dense] = values[last];
            slots[slotOf(movedKey)] = dense + 1;
        }
        values[last] = null;
        size = last;
        return removed;
    }

    void clear() {
        Arrays.fill(values, 0, size, null);
        size = 0;
        occupied = 0;
        Arrays.fill(slots, FREE);
    }

    private int slotOf(final long key) {
        int index = hash(key) & mask;
        while (true) {
            final int entry = slots[index];
            if (entry == FREE) {
                return -1;
            }
            if (entry != TOMBSTONE && keys[entry - 1] == key) {
                return index;
            }
            index = (index + 1) & mask;
        }
    }

    private void insertSlot(final long key, final int denseIndexPlusOne) {
        int index = hash(key) & mask;
        while (slots[index] > 0) {
            index = (index + 1) & mask;
        }
        if (slots[index] == FREE) {
            occupied++;
        }
        slots[index] = denseIndexPlusOne;
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

    private static int hash(final long key) {
        final long spread = key * 0x9E3779B97F4A7C15L;
        return (int) (spread >>> 32) ^ (int) spread;
    }
}
