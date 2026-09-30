package io.github.green4j.sdsm;

/**
 * Reads the records of a batch in order. The cursor and everything it returns - the text of a
 * value included - are valid only until the next {@link #next()} and never outlive the
 * {@code onBatch} call that handed the batch over.
 */
public final class ChangeCursor {

    private final ChangeBuffer buffer;
    private final TextView text = new TextView();

    private int record = -1;
    private int count;

    ChangeCursor(final ChangeBuffer buffer) {
        this.buffer = buffer;
    }

    void reset() {
        record = -1;
        count = buffer.recordCount();
    }

    /**
     * @return true if a record is now under the cursor, false when the batch is exhausted
     */
    public boolean next() {
        if (record + 1 >= count) {
            return false;
        }
        record++;
        return true;
    }

    public ChangeKind changeKind() {
        return buffer.changeKindAt(record);
    }

    public long objectId() {
        return buffer.objectIdAt(record);
    }

    public ObjectKind objectKind() {
        return buffer.objectKindAt(record);
    }

    /**
     * @return the parent of a {@link ChangeKind#CONTAINED} or {@link ChangeKind#UNCONTAINED}
     *         record; the object under the cursor is the child
     */
    public long parentId() {
        return buffer.numberAt(record);
    }

    /**
     * @return the key id of a {@link ChangeKind#PROPERTY_CHANGED} record
     */
    public int propertyKeyId() {
        return buffer.keyIdAt(record);
    }

    /**
     * @return the name the key id stands for, carried by the batch itself
     */
    public String propertyKey() {
        return buffer.keyName(buffer.keyIdAt(record));
    }

    /**
     * @return what the value holds, {@link ValueType#ABSENT} when the property was removed
     */
    public ValueType valueType() {
        return buffer.valueTypeAt(record);
    }

    public long longValue() {
        return buffer.numberAt(record);
    }

    public double doubleValue() {
        return Double.longBitsToDouble(buffer.numberAt(record));
    }

    public boolean booleanValue() {
        return buffer.numberAt(record) != 0L;
    }

    /**
     * @return the text of the value, backed by the batch and invalidated by {@link #next()}
     */
    public CharSequence textValue() {
        text.wrap(buffer.textChars(), buffer.textOffsetAt(record), buffer.textLengthAt(record));
        return text;
    }

    private static final class TextView implements CharSequence {
        private char[] source;
        private int offset;
        private int length;

        void wrap(final char[] newSource, final int newOffset, final int newLength) {
            this.source = newSource;
            this.offset = newOffset;
            this.length = newLength;
        }

        @Override
        public int length() {
            return length;
        }

        @Override
        public char charAt(final int index) {
            return source[offset + index];
        }

        @Override
        public CharSequence subSequence(final int start, final int end) {
            final TextView slice = new TextView();
            slice.wrap(source, offset + start, end - start);
            return slice;
        }

        @Override
        public String toString() {
            return new String(source, offset, length);
        }
    }
}
