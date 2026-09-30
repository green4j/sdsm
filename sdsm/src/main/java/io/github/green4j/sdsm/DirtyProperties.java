package io.github.green4j.sdsm;

import java.util.Arrays;

/**
 * The properties a view owes its subscribers, as (object id, key id) pairs in the order they
 * first changed. A pair is held once however often it changes, so the delivered value is the
 * one the structure holds at drain time and never an intermediate one - which is also what a
 * snapshot of the same view would say.
 */
final class DirtyProperties {

    private static final int FREE = 0;

    private long[] objectIds = new long[64];
    private int[] keyIds = new int[64];
    /** Where each pair sits in the slot table, so clearing costs the pairs held, not the table. */
    private int[] slotIndexes = new int[64];
    private int size;

    private int[] slots = new int[256];
    private int mask = slots.length - 1;
    private int occupied;

    int size() {
        return size;
    }

    long objectIdAt(final int index) {
        return objectIds[index];
    }

    int keyIdAt(final int index) {
        return keyIds[index];
    }

    boolean mark(final long objectId, final int keyId) {
        if (slotOf(objectId, keyId) >= 0) {
            return false;
        }
        if (size == objectIds.length) {
            objectIds = Arrays.copyOf(objectIds, size * 2);
            keyIds = Arrays.copyOf(keyIds, size * 2);
            slotIndexes = Arrays.copyOf(slotIndexes, size * 2);
        }
        objectIds[size] = objectId;
        keyIds[size] = keyId;
        size++;
        insertSlot(objectId, keyId, size);
        if (occupied * 4 > slots.length * 3) {
            rehash();
        }
        return true;
    }

    void clear() {
        for (int i = 0; i < size; i++) {
            slots[slotIndexes[i]] = FREE;
        }
        size = 0;
        occupied = 0;
    }

    private int slotOf(final long objectId, final int keyId) {
        int index = hash(objectId, keyId) & mask;
        while (true) {
            final int entry = slots[index];
            if (entry == FREE) {
                return -1;
            }
            if (objectIds[entry - 1] == objectId && keyIds[entry - 1] == keyId) {
                return index;
            }
            index = (index + 1) & mask;
        }
    }

    private void insertSlot(final long objectId, final int keyId, final int denseIndexPlusOne) {
        int index = hash(objectId, keyId) & mask;
        while (slots[index] != FREE) {
            index = (index + 1) & mask;
        }
        occupied++;
        slots[index] = denseIndexPlusOne;
        slotIndexes[denseIndexPlusOne - 1] = index;
    }

    private void rehash() {
        final int capacity = Math.max(256, Integer.highestOneBit(size * 4 - 1) * 2);
        slots = new int[capacity];
        mask = capacity - 1;
        occupied = 0;
        for (int i = 0; i < size; i++) {
            insertSlot(objectIds[i], keyIds[i], i + 1);
        }
    }

    private static int hash(final long objectId, final int keyId) {
        final long spread = (objectId * 0x9E3779B97F4A7C15L) ^ (keyId * 0xC2B2AE3D27D4EB4FL);
        return (int) (spread >>> 32) ^ (int) spread;
    }
}
