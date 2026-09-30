package io.github.green4j.sdsm;

import java.util.ArrayList;
import java.util.List;

/**
 * Collects what a view delivers. A batch is lent only for the duration of the call, so the
 * records are copied out here - which is what any subscriber has to do.
 */
final class Recorder implements BatchSubscriber {

    private static final class Change {
        private final boolean fromSnapshot;
        private final ChangeKind kind;
        private final long objectId;
        private final long groupId;
        private final String key;
        private final ValueType valueType;
        private final String value;

        Change(final boolean fromSnapshot,
                final ChangeKind kind,
                final long objectId,
                final long groupId,
                final String key,
                final ValueType valueType,
                final String value) {
            this.fromSnapshot = fromSnapshot;
            this.kind = kind;
            this.objectId = objectId;
            this.groupId = groupId;
            this.key = key;
            this.valueType = valueType;
            this.value = value;
        }
    }

    private final List<Change> changes = new ArrayList<>();
    private final List<String> sequence = new ArrayList<>();
    private int batches;
    private Throwable failure;

    @Override
    public void onBatch(final StructureBatch batch) {
        batches++;
        sequence.add((batch.isInitialSnapshot() ? "S" : "D") + batch.batchSequenceNumber());
        final ChangeCursor cursor = batch.cursor();
        while (cursor.next()) {
            final boolean property = cursor.changeKind() == ChangeKind.PROPERTY_CHANGED;
            changes.add(new Change(
                    batch.isInitialSnapshot(),
                    cursor.changeKind(),
                    cursor.objectId(),
                    property ? 0L : cursor.parentId(),
                    property ? cursor.propertyKey() : null,
                    property ? cursor.valueType() : null,
                    property ? textOf(cursor) : null));
        }
    }

    @Override
    public void onError(final Throwable reason) {
        failure = reason;
    }

    int batches() {
        return batches;
    }

    /**
     * @return the batches in the order they came, as {@code S<seq>} for a snapshot and
     *         {@code D<seq>} for a delta
     */
    List<String> sequence() {
        return sequence;
    }

    Throwable failure() {
        return failure;
    }

    List<Long> idsInSnapshot(final ChangeKind kind) {
        return ids(kind, true);
    }

    List<Long> idsAfterSnapshot(final ChangeKind kind) {
        return ids(kind, false);
    }

    List<String> keysInSnapshot() {
        final List<String> keys = new ArrayList<>();
        for (final Change change : changes) {
            if (change.fromSnapshot && change.kind == ChangeKind.PROPERTY_CHANGED) {
                keys.add(change.key);
            }
        }
        return keys;
    }

    int recordsFor(final long objectId, final String key) {
        int count = 0;
        for (final Change change : changes) {
            if (change.objectId == objectId && key.equals(change.key)) {
                count++;
            }
        }
        return count;
    }

    ValueType typeOf(final long objectId, final String key) {
        ValueType latest = null;
        for (final Change change : changes) {
            if (change.objectId == objectId && key.equals(change.key)) {
                latest = change.valueType;
            }
        }
        return latest;
    }

    String latestValueOf(final long objectId, final String key) {
        String latest = null;
        for (final Change change : changes) {
            if (change.objectId == objectId && key.equals(change.key)) {
                latest = change.value;
            }
        }
        return latest;
    }

    /**
     * @param kind CONTAINED or UNCONTAINED
     * @return the pairs stated, as member-in-group, in the order they arrived
     */
    List<String> membership(final ChangeKind kind) {
        final List<String> pairs = new ArrayList<>();
        for (final Change change : changes) {
            if (change.kind == kind) {
                pairs.add(change.objectId + " in " + change.groupId);
            }
        }
        return pairs;
    }

    /**
     * @return whether the snapshot said everything was there before it said anything else -
     *         which is what makes every record naming two objects readable
     */
    boolean snapshotSaidWhatIsThereFirst() {
        boolean saidSomethingElse = false;
        for (final Change change : changes) {
            if (!change.fromSnapshot) {
                continue;
            }
            if (change.kind == ChangeKind.ADDED) {
                if (saidSomethingElse) {
                    return false;
                }
            } else {
                saidSomethingElse = true;
            }
        }
        return true;
    }

    /**
     * @return whether every record naming two objects arrived after both of them were said to
     *         be there - the only order in which a receiver holding nothing can read one
     */
    boolean everyPairFollowsItsEnds() {
        final List<Long> present = new ArrayList<>();
        for (final Change change : changes) {
            if (change.kind == ChangeKind.ADDED) {
                present.add(Long.valueOf(change.objectId));
                continue;
            }
            if (change.kind != ChangeKind.CONTAINED) {
                continue;
            }
            if (!present.contains(Long.valueOf(change.objectId))
                    || !present.contains(Long.valueOf(change.groupId))) {
                return false;
            }
        }
        return true;
    }

    private List<Long> ids(final ChangeKind kind, final boolean fromSnapshot) {
        final List<Long> ids = new ArrayList<>();
        for (final Change change : changes) {
            if (change.kind == kind && change.fromSnapshot == fromSnapshot) {
                ids.add(Long.valueOf(change.objectId));
            }
        }
        return ids;
    }

    private static String textOf(final ChangeCursor cursor) {
        switch (cursor.valueType()) {
            case LONG:
                return Long.toString(cursor.longValue());
            case DOUBLE:
                return Double.toString(cursor.doubleValue());
            case BOOLEAN:
                return Boolean.toString(cursor.booleanValue());
            case TEXT:
                return cursor.textValue().toString();
            default:
                return null;
        }
    }
}
