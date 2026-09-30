package io.github.green4j.sdsm;

/**
 * Typed property values of one object, keyed by property key id. Dense parallel arrays in
 * insertion order: an object carries a handful of properties, so a scan beats a hash, and the
 * order is the order a snapshot delivers them in.
 * <p>
 * Every value carries the id of the source that wrote it, so a source can later tell apart
 * what it is answerable for from what another source put on the same object.
 */
final class PropertyStore {

    private static final int INITIAL_CAPACITY = 4;

    private int[] keyIds = new int[INITIAL_CAPACITY];
    private ValueType[] types = new ValueType[INITIAL_CAPACITY];
    private long[] numbers = new long[INITIAL_CAPACITY];
    private String[] texts = new String[INITIAL_CAPACITY];
    private int[] sources = new int[INITIAL_CAPACITY];
    private int size;

    int size() {
        return size;
    }

    int keyIdAt(final int index) {
        return keyIds[index];
    }

    ValueType typeOf(final int keyId) {
        final int index = indexOf(keyId);
        return index < 0 ? ValueType.ABSENT : types[index];
    }

    long numberOf(final int keyId) {
        final int index = indexOf(keyId);
        return index < 0 ? 0L : numbers[index];
    }

    String textOf(final int keyId) {
        final int index = indexOf(keyId);
        return index < 0 ? null : texts[index];
    }

    int sourceOf(final int keyId) {
        final int index = indexOf(keyId);
        return index < 0 ? Structure.NO_SOURCE : sources[index];
    }

    /**
     * A value written over the one already there is not a change. The source is remembered
     * either way - who last said it matters to the feed that may have to take it back - but a
     * write that says the same thing is not something anyone needs to be told.
     *
     * @param keyId    the property
     * @param value    the value
     * @param sourceId who wrote it
     * @return whether what is held now differs from what was held before
     */
    boolean setLong(final int keyId, final long value, final int sourceId) {
        final int index = slotFor(keyId);
        final boolean changed = types[index] != ValueType.LONG || numbers[index] != value;
        types[index] = ValueType.LONG;
        numbers[index] = value;
        texts[index] = null;
        sources[index] = sourceId;
        return changed;
    }

    boolean setDouble(final int keyId, final double value, final int sourceId) {
        final int index = slotFor(keyId);
        final long bits = Double.doubleToRawLongBits(value);
        final boolean changed = types[index] != ValueType.DOUBLE || numbers[index] != bits;
        types[index] = ValueType.DOUBLE;
        numbers[index] = bits;
        texts[index] = null;
        sources[index] = sourceId;
        return changed;
    }

    boolean setBoolean(final int keyId, final boolean value, final int sourceId) {
        final int index = slotFor(keyId);
        final long number = value ? 1L : 0L;
        final boolean changed = types[index] != ValueType.BOOLEAN || numbers[index] != number;
        types[index] = ValueType.BOOLEAN;
        numbers[index] = number;
        texts[index] = null;
        sources[index] = sourceId;
        return changed;
    }

    boolean setText(final int keyId, final String value, final int sourceId) {
        final int index = slotFor(keyId);
        final boolean changed = types[index] != ValueType.TEXT || !value.equals(texts[index]);
        types[index] = ValueType.TEXT;
        numbers[index] = 0L;
        texts[index] = value;
        sources[index] = sourceId;
        return changed;
    }

    boolean remove(final int keyId) {
        final int index = indexOf(keyId);
        if (index < 0) {
            return false;
        }
        final int tail = size - index - 1;
        if (tail > 0) {
            System.arraycopy(keyIds, index + 1, keyIds, index, tail);
            System.arraycopy(types, index + 1, types, index, tail);
            System.arraycopy(numbers, index + 1, numbers, index, tail);
            System.arraycopy(texts, index + 1, texts, index, tail);
            System.arraycopy(sources, index + 1, sources, index, tail);
        }
        size--;
        texts[size] = null;
        return true;
    }

    private int indexOf(final int keyId) {
        for (int i = 0; i < size; i++) {
            if (keyIds[i] == keyId) {
                return i;
            }
        }
        return -1;
    }

    private int slotFor(final int keyId) {
        final int existing = indexOf(keyId);
        if (existing >= 0) {
            return existing;
        }
        if (size == keyIds.length) {
            grow();
        }
        keyIds[size] = keyId;
        types[size] = ValueType.ABSENT;
        texts[size] = null;
        return size++;
    }

    private void grow() {
        final int capacity = keyIds.length * 2;
        final int[] grownKeyIds = new int[capacity];
        final ValueType[] grownTypes = new ValueType[capacity];
        final long[] grownNumbers = new long[capacity];
        final String[] grownTexts = new String[capacity];
        final int[] grownSources = new int[capacity];
        System.arraycopy(keyIds, 0, grownKeyIds, 0, size);
        System.arraycopy(types, 0, grownTypes, 0, size);
        System.arraycopy(numbers, 0, grownNumbers, 0, size);
        System.arraycopy(texts, 0, grownTexts, 0, size);
        System.arraycopy(sources, 0, grownSources, 0, size);
        keyIds = grownKeyIds;
        types = grownTypes;
        numbers = grownNumbers;
        texts = grownTexts;
        sources = grownSources;
    }
}
