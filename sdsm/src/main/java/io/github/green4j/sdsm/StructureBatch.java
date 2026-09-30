package io.github.green4j.sdsm;

/**
 * One delivery to a view's subscribers: the records the view owes them, in order, over a
 * buffer the engine owns.
 * <p>
 * A batch is lent to {@link BatchSubscriber#onBatch(StructureBatch)} and taken back when the
 * call returns - the engine then clears it and reuses it for the next delivery. A subscriber
 * that needs anything after the call must copy it out.
 */
public final class StructureBatch {

    private final ChangeBuffer buffer = new ChangeBuffer();
    private final ChangeCursor cursor = new ChangeCursor(buffer);

    private long batchSequenceNumber;
    private long structureVersion;
    private long emittedAtNanos;
    private boolean initialSnapshot;
    private BatchSubscriber target;
    private boolean targetSubscribed;

    StructureBatch() {
    }

    ChangeBuffer buffer() {
        return buffer;
    }

    void stamp(final long newBatchSequenceNumber,
               final long newStructureVersion,
               final long newEmittedAtNanos,
               final boolean newInitialSnapshot) {
        this.batchSequenceNumber = newBatchSequenceNumber;
        this.structureVersion = newStructureVersion;
        this.emittedAtNanos = newEmittedAtNanos;
        this.initialSnapshot = newInitialSnapshot;
    }

    /**
     * @param subscriber the only one to send it to
     * @param subscribed whether it is a subscriber of the view, rather than someone asking once
     */
    void targetOnly(final BatchSubscriber subscriber, final boolean subscribed) {
        this.target = subscriber;
        this.targetSubscribed = subscribed;
    }

    BatchSubscriber target() {
        return target;
    }

    boolean targetSubscribed() {
        return targetSubscribed;
    }

    void reset() {
        buffer.clear();
        batchSequenceNumber = 0L;
        structureVersion = 0L;
        emittedAtNanos = 0L;
        initialSnapshot = false;
        target = null;
        targetSubscribed = false;
    }

    /**
     * @return for a delta, one more than the batch its subscriber was sent before it; for a
     *         snapshot, the number of the last delta it includes. A subscriber is sent a snapshot
     *         and then every delta after it, with no gap and nothing repeated.
     */
    public long batchSequenceNumber() {
        return batchSequenceNumber;
    }

    public long structureVersion() {
        return structureVersion;
    }

    public long emittedAtNanos() {
        return emittedAtNanos;
    }

    /**
     * @return true when this batch is a complete statement of what the view holds, built from
     *         current state, rather than what changed since the last one
     */
    public boolean isInitialSnapshot() {
        return initialSnapshot;
    }

    public int size() {
        return buffer.recordCount();
    }

    public boolean isEmpty() {
        return buffer.recordCount() == 0;
    }

    /**
     * @return this batch's cursor, positioned before the first record
     */
    public ChangeCursor cursor() {
        cursor.reset();
        return cursor;
    }
}
