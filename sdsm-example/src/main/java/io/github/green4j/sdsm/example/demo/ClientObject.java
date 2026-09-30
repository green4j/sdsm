package io.github.green4j.sdsm.example.demo;

import io.github.green4j.sdsm.ObjectKind;
import io.github.green4j.sdsm.ValueType;

/**
 * One object of a {@link ClientView}, as the batches describe it. What an object is - its
 * name, the node a port sits on, the ports a link joins - arrives under the same keys as
 * everything else, so this holds the intrinsics in fields and the rest in a store of its own,
 * and neither is read from the structure.
 */
public final class ClientObject {

    private static final long[] NO_IDS = new long[0];

    private final long id;
    private final ObjectKind kind;

    private String name = "";
    private String type = "";
    private String externalId;
    private String axis;
    private String address;
    private long node = -1L;
    private long from = -1L;
    private long to = -1L;

    private String[] keys = new String[4];
    private ValueType[] types = new ValueType[4];
    private long[] numbers = new long[4];
    private String[] texts = new String[4];
    private int propertyCount;

    private long[] groups = NO_IDS;
    private int groupCount;
    private long[] members = NO_IDS;
    private int memberCount;

    ClientObject(final long id, final ObjectKind kind) {
        this.id = id;
        this.kind = kind;
    }

    public long id() {
        return id;
    }

    public ObjectKind kind() {
        return kind;
    }

    public String name() {
        return name;
    }

    public String type() {
        return type;
    }

    public String externalId() {
        return externalId;
    }

    public String axis() {
        return axis;
    }

    public String address() {
        return address;
    }

    /**
     * @return the node a port sits on, -1 if this is not a port
     */
    public long node() {
        return node;
    }

    /**
     * @return the output a link starts at, -1 if this is not a link
     */
    public long from() {
        return from;
    }

    /**
     * @return the input a link ends at, -1 if this is not a link
     */
    public long to() {
        return to;
    }

    public boolean has(final String key) {
        return indexOf(key) >= 0;
    }

    public long longOf(final String key) {
        final int at = indexOf(key);
        return at < 0 ? 0L : numbers[at];
    }

    public double doubleOf(final String key) {
        final int at = indexOf(key);
        return at < 0 ? 0.0 : Double.longBitsToDouble(numbers[at]);
    }

    public boolean booleanOf(final String key) {
        final int at = indexOf(key);
        return at >= 0 && numbers[at] != 0L;
    }

    public String textOf(final String key) {
        final int at = indexOf(key);
        return at < 0 ? null : texts[at];
    }

    public int groupCount() {
        return groupCount;
    }

    public long groupAt(final int index) {
        return groups[index];
    }

    public int memberCount() {
        return memberCount;
    }

    public long memberAt(final int index) {
        return members[index];
    }

    void set(final String key,
             final ValueType valueType,
             final long number,
             final String text) {
        switch (key) {
            case "name":
                name = text;
                return;
            case "type":
                type = text;
                return;
            case "externalId":
                externalId = text;
                return;
            case "axis":
                axis = text;
                return;
            case "address":
                address = text;
                return;
            case "node":
                node = number;
                return;
            case "from":
                from = number;
                return;
            case "to":
                to = number;
                return;
            case "id":
                return;
            case "kind":
                return;
            default:
                store(key, valueType, number, text);
        }
    }

    void joined(final long groupId) {
        if (holds(groups, groupCount, groupId)) {
            return;
        }
        groups = grown(groups, groupCount);
        groups[groupCount++] = groupId;
    }

    void left(final long groupId) {
        groupCount = without(groups, groupCount, groupId);
    }

    void gained(final long memberId) {
        if (holds(members, memberCount, memberId)) {
            return;
        }
        members = grown(members, memberCount);
        members[memberCount++] = memberId;
    }

    void lost(final long memberId) {
        memberCount = without(members, memberCount, memberId);
    }

    private void store(final String key,
                       final ValueType valueType,
                       final long number,
                       final String text) {
        final int at = indexOf(key);
        if (valueType == ValueType.ABSENT) {
            if (at >= 0) {
                propertyCount--;
                keys[at] = keys[propertyCount];
                types[at] = types[propertyCount];
                numbers[at] = numbers[propertyCount];
                texts[at] = texts[propertyCount];
                keys[propertyCount] = null;
                texts[propertyCount] = null;
            }
            return;
        }
        final int slot = at >= 0 ? at : reserve(key);
        types[slot] = valueType;
        numbers[slot] = number;
        texts[slot] = text;
    }

    private int reserve(final String key) {
        if (propertyCount == keys.length) {
            final String[] grownKeys = new String[propertyCount * 2];
            final ValueType[] grownTypes = new ValueType[propertyCount * 2];
            final long[] grownNumbers = new long[propertyCount * 2];
            final String[] grownTexts = new String[propertyCount * 2];
            System.arraycopy(keys, 0, grownKeys, 0, propertyCount);
            System.arraycopy(types, 0, grownTypes, 0, propertyCount);
            System.arraycopy(numbers, 0, grownNumbers, 0, propertyCount);
            System.arraycopy(texts, 0, grownTexts, 0, propertyCount);
            keys = grownKeys;
            types = grownTypes;
            numbers = grownNumbers;
            texts = grownTexts;
        }
        keys[propertyCount] = key;
        return propertyCount++;
    }

    private int indexOf(final String key) {
        for (int i = 0; i < propertyCount; i++) {
            if (keys[i].equals(key)) {
                return i;
            }
        }
        return -1;
    }

    private static boolean holds(final long[] ids, final int count, final long id) {
        for (int i = 0; i < count; i++) {
            if (ids[i] == id) {
                return true;
            }
        }
        return false;
    }

    private static long[] grown(final long[] ids, final int count) {
        if (count < ids.length) {
            return ids;
        }
        final long[] wider = new long[count == 0 ? 4 : count * 2];
        System.arraycopy(ids, 0, wider, 0, count);
        return wider;
    }

    private static int without(final long[] ids, final int count, final long id) {
        for (int i = 0; i < count; i++) {
            if (ids[i] == id) {
                ids[i] = ids[count - 1];
                return count - 1;
            }
        }
        return count;
    }
}
