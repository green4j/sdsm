package io.github.green4j.sdsm;

import java.util.Arrays;

/**
 * How many claims each object id is under. An object materialized for two observations - a
 * node both clusters put themselves in - must outlive the first of them, so a claim is
 * counted, not merely held.
 * <p>
 * Same shape as {@link LongSet}: a dense array beside its counts, and an open-addressed slot
 * table to find one.
 */
final class LongCounts {

    private static final int FREE = 0;
    private static final int TOMBSTONE = -1;

    private long[] values = new long[16];
    private int[] counts = new int[16];
    private int size;

    private int[] slots = new int[64];
    private int mask = slots.length - 1;
    private int occupied;

    int countOf(final long value) {
        final int slot = slotOf(value);
        return slot < 0 ? 0 : counts[slots[slot] - 1];
    }

    void claim(final long value) {
        final int slot = slotOf(value);
        if (slot >= 0) {
            counts[slots[slot] - 1]++;
            return;
        }
        if (size == values.length) {
            values = Arrays.copyOf(values, size * 2);
            counts = Arrays.copyOf(counts, size * 2);
        }
        values[size] = value;
        counts[size] = 1;
        size++;
        insertSlot(value, size);
        if (occupied * 4 > slots.length * 3) {
            rehash();
        }
    }

    /**
     * @param value the id to let go of
     * @return whether that was the last claim on it
     */
    boolean release(final long value) {
        final int slot = slotOf(value);
        if (slot < 0) {
            return false;
        }
        final int dense = slots[slot] - 1;
        if (--counts[dense] > 0) {
            return false;
        }
        removeAt(slot, dense);
        return true;
    }

    void clear() {
        size = 0;
        occupied = 0;
        Arrays.fill(slots, FREE);
    }

    private void removeAt(final int slot, final int dense) {
        slots[slot] = TOMBSTONE;
        final int last = size - 1;
        if (dense != last) {
            final long moved = values[last];
            values[dense] = moved;
            counts[dense] = counts[last];
            slots[slotOf(moved)] = dense + 1;
        }
        size = last;
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
