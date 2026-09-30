package io.github.green4j.sdsm;

/**
 * The flat form of a batch: fixed-width records in a {@code long[]}, text in a {@code char[]}
 * alongside as offset and length. Appending costs no allocation once the arrays are large
 * enough, and a batch is reused by clearing it rather than by building a new one.
 * <p>
 * A batch also carries the names of the keys it mentions, so a receiver can read it without
 * asking the structure what a key id stands for.
 */
final class ChangeBuffer {

    private static final int RECORD_WORDS = 5;

    private static final int HEADER = 0;
    private static final int OBJECT_ID = 1;
    private static final int KEY_ID = 2;
    private static final int NUMBER = 3;
    private static final int TEXT = 4;

    private static final ObjectKind[] OBJECT_KINDS = ObjectKind.values();
    private static final ChangeKind[] CHANGE_KINDS = ChangeKind.values();
    private static final ValueType[] VALUE_TYPES = ValueType.values();

    private long[] words = new long[RECORD_WORDS * 64];
    private int wordCount;
    private char[] chars = new char[512];
    private int charCount;
    private int[] tableKeyIds = new int[16];
    private String[] tableNames = new String[16];
    private int tableSize;

    int recordCount() {
        return wordCount / RECORD_WORDS;
    }

    void appendStructural(final ChangeKind changeKind,
                          final long objectId,
                          final ObjectKind objectKind) {
        final int at = reserveRecord();
        words[at + HEADER] = header(objectKind, changeKind, ValueType.ABSENT);
        words[at + OBJECT_ID] = objectId;
        words[at + KEY_ID] = 0L;
        words[at + NUMBER] = 0L;
        words[at + TEXT] = 0L;
    }

    void appendMembership(final ChangeKind changeKind,
                          final long memberId,
                          final ObjectKind memberKind,
                          final long groupId) {
        final int at = reserveRecord();
        words[at + HEADER] = header(memberKind, changeKind, ValueType.ABSENT);
        words[at + OBJECT_ID] = memberId;
        words[at + KEY_ID] = 0L;
        words[at + NUMBER] = groupId;
        words[at + TEXT] = 0L;
    }

    void appendProperty(final long objectId,
                        final ObjectKind objectKind,
                        final int keyId,
                        final String keyName,
                        final ValueType valueType,
                        final long number,
                        final CharSequence text) {
        rememberKey(keyId, keyName);
        final int at = reserveRecord();
        words[at + HEADER] = header(objectKind, ChangeKind.PROPERTY_CHANGED, valueType);
        words[at + OBJECT_ID] = objectId;
        words[at + KEY_ID] = keyId;
        words[at + NUMBER] = number;
        words[at + TEXT] = valueType == ValueType.TEXT ? appendText(text) : 0L;
    }

    ObjectKind objectKindAt(final int record) {
        return OBJECT_KINDS[(int) (words[record * RECORD_WORDS + HEADER] & 0xFFL)];
    }

    ChangeKind changeKindAt(final int record) {
        return CHANGE_KINDS[(int) ((words[record * RECORD_WORDS + HEADER] >>> 8) & 0xFFL)];
    }

    ValueType valueTypeAt(final int record) {
        return VALUE_TYPES[(int) ((words[record * RECORD_WORDS + HEADER] >>> 16) & 0xFFL)];
    }

    long objectIdAt(final int record) {
        return words[record * RECORD_WORDS + OBJECT_ID];
    }

    int keyIdAt(final int record) {
        return (int) words[record * RECORD_WORDS + KEY_ID];
    }

    long numberAt(final int record) {
        return words[record * RECORD_WORDS + NUMBER];
    }

    int textOffsetAt(final int record) {
        return (int) (words[record * RECORD_WORDS + TEXT] >>> 32);
    }

    int textLengthAt(final int record) {
        return (int) words[record * RECORD_WORDS + TEXT];
    }

    char[] textChars() {
        return chars;
    }

    String keyName(final int keyId) {
        for (int i = 0; i < tableSize; i++) {
            if (tableKeyIds[i] == keyId) {
                return tableNames[i];
            }
        }
        return null;
    }

    void clear() {
        wordCount = 0;
        charCount = 0;
        for (int i = 0; i < tableSize; i++) {
            tableNames[i] = null;
        }
        tableSize = 0;
    }

    private static long header(final ObjectKind objectKind,
                               final ChangeKind changeKind,
                               final ValueType valueType) {
        return objectKind.ordinal()
                | ((long) changeKind.ordinal() << 8)
                | ((long) valueType.ordinal() << 16);
    }

    private int reserveRecord() {
        if (wordCount + RECORD_WORDS > words.length) {
            final long[] grown = new long[words.length * 2];
            System.arraycopy(words, 0, grown, 0, wordCount);
            words = grown;
        }
        final int at = wordCount;
        wordCount += RECORD_WORDS;
        return at;
    }

    private long appendText(final CharSequence text) {
        if (text == null) {
            return 0L;
        }
        final int length = text.length();
        while (charCount + length > chars.length) {
            final char[] grown = new char[chars.length * 2];
            System.arraycopy(chars, 0, grown, 0, charCount);
            chars = grown;
        }
        if (text instanceof String) {
            ((String) text).getChars(0, length, chars, charCount);
        } else {
            for (int i = 0; i < length; i++) {
                chars[charCount + i] = text.charAt(i);
            }
        }
        final long encoded = ((long) charCount << 32) | length;
        charCount += length;
        return encoded;
    }

    private void rememberKey(final int keyId, final String keyName) {
        for (int i = 0; i < tableSize; i++) {
            if (tableKeyIds[i] == keyId) {
                return;
            }
        }
        if (tableSize == tableKeyIds.length) {
            final int[] grownIds = new int[tableSize * 2];
            final String[] grownNames = new String[tableSize * 2];
            System.arraycopy(tableKeyIds, 0, grownIds, 0, tableSize);
            System.arraycopy(tableNames, 0, grownNames, 0, tableSize);
            tableKeyIds = grownIds;
            tableNames = grownNames;
        }
        tableKeyIds[tableSize] = keyId;
        tableNames[tableSize] = keyName;
        tableSize++;
    }
}
