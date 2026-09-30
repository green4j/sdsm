package io.github.green4j.sdsm;

import java.util.Arrays;

/**
 * A set of object ids: a dense array to walk and an open-addressed slot table to test
 * membership. {@code Set<Long>} would box on every test, and membership is tested once per
 * view on every property change.
 * <p>
 * Iteration order is the order of the dense array, which a removal disturbs: order is not part
 * of the contract.
 */
final class LongSet {

    private static final int FREE = 0;
    private static final int TOMBSTONE = -1;

    private long[] values = new long[16];
    private int size;

    private int[] slots = new int[64];
    private int mask = slots.length - 1;
    private int occupied;

    int size() {
        return size;
    }

    long valueAt(final int index) {
        return values[index];
    }

    boolean contains(final long value) {
        return slotOf(value) >= 0;
    }

    boolean add(final long value) {
        if (slotOf(value) >= 0) {
            return false;
        }
        if (size == values.length) {
            values = Arrays.copyOf(values, size * 2);
        }
        values[size] = value;
        size++;
        insertSlot(value, size);
        if (occupied * 4 > slots.length * 3) {
            rehash();
        }
        return true;
    }

    boolean remove(final long value) {
        final int slot = slotOf(value);
        if (slot < 0) {
            return false;
        }
        final int dense = slots[slot] - 1;
        slots[slot] = TOMBSTONE;
        final int last = size - 1;
        if (dense != last) {
            final long moved = values[last];
            values[dense] = moved;
            slots[slotOf(moved)] = dense + 1;
        }
        size = last;
        return true;
    }

    void clear() {
        size = 0;
        occupied = 0;
        Arrays.fill(slots, FREE);
    }

    private int slotOf(final long value) {
        int index = hash(value) & mask;
        while (true) {
            final int entry = slots[index];
            if (entry == FREE) {
                return -1;
            }
            if (entry != TOMBSTONE && values[entry - 1] == value) {
                return index;
            }
            index = (index + 1) & mask;
        }
    }

    private void insertSlot(final long value, final int denseIndexPlusOne) {
        int index = hash(value) & mask;
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
            insertSlot(values[i], i + 1);
        }
    }

    private static int hash(final long value) {
        final long spread = value * 0x9E3779B97F4A7C15L;
        return (int) (spread >>> 32) ^ (int) spread;
    }
}
