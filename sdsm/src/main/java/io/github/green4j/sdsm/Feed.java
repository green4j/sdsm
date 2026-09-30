package io.github.green4j.sdsm;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Supplier;

/**
 * One source pushing into one loop. A source offers what it sees from whatever thread it sees
 * it on; the loop keeps the last observation per id and hands the lot to the structure, in one
 * task with whatever else its thread has ready. While that task is in flight the loop goes on
 * coalescing,
 * so a source that talks faster than the structure writes costs batches, not queue.
 * <p>
 * What an observation says is materialized only when its version is not the one already
 * materialized for that id. What an area no longer holds is swept when the source says the
 * area is complete, and nothing is swept while the feed is stale: an unseen thing is not a
 * thing that has gone.
 *
 * @param <O> what its source observes
 */
public final class Feed<O extends Observation> {

    private static final byte OBSERVED = 0;
    private static final byte REMOVED = 1;
    private static final byte COMPLETE = 2;
    private static final byte UNAVAILABLE = 3;
    private static final byte AVAILABLE = 4;

    private static final int[] NO_THRESHOLDS = new int[0];

    private final EventLoop loop;
    private final Structure structure;
    private final Source<O> source;
    private final Materializer<O> materializer;
    private final int sourceId;
    private final Emit emit;
    private final LongCounts claims;
    private final int[] supplyThresholds;
    private final DemandListener demand = this::demandChanged;

    private final Object intakeLock = new Object();
    private Batch filling = new Batch();
    private Batch draining = new Batch();
    private boolean scheduled;
    private boolean inFlight;
    private boolean closing;

    private final Runnable drainTask = this::drainOnLoop;
    private final Runnable applyTask = this::applyBatch;
    private final Supplier<Void> closeTask = this::closeOnStructure;
    private final TextObjectMap<Object> coalesced = new TextObjectMap<>();
    private Throwable applyFailure;

    private final TextObjectMap<Entry> entries = new TextObjectMap<>();
    private final LongObjectMap<Area> areas = new LongObjectMap<>();
    private boolean converged;

    private volatile FeedState state = FeedState.LOADING;

    private final Object demandLock = new Object();
    private final Runnable demandTask = this::demandOnLoop;
    private Levels pending = new Levels();
    private Levels telling = new Levels();
    private boolean demandScheduled;

    Feed(final EventLoop loop,
            final Structure structure,
            final int sourceId,
            final Source<O> source,
            final Materializer<O> materializer) {
        this.loop = loop;
        this.structure = structure;
        this.claims = structure.claims();
        this.sourceId = sourceId;
        this.source = source;
        this.materializer = materializer;
        this.supplyThresholds = thresholdsOf(structure, source.suppliedProperties());
        this.emit = new Emit(structure, supplyThresholds);
        structure.watchDemand(demand);
    }

    private static int[] thresholdsOf(final Structure structure,
                                      final Map<String, DetailLevel> supplied) {
        if (supplied.isEmpty()) {
            return NO_THRESHOLDS;
        }
        final PropertyKeys keys = structure.propertyKeys();
        int[] table = NO_THRESHOLDS;
        for (final Map.Entry<String, DetailLevel> declared : supplied.entrySet()) {
            final int keyId = keys.idOf(declared.getKey());
            if (PropertyKeys.isReserved(keyId)) {
                throw new IllegalArgumentException(
                        "A source cannot hold back an intrinsic property: " + declared.getKey());
            }
            if (keyId >= table.length) {
                table = Arrays.copyOf(table, keyId + 1);
            }
            table[keyId] = declared.getValue().ordinal();
        }
        return table;
    }

    /**
     * @return where the feed is now
     */
    public FeedState state() {
        return state;
    }

    /**
     * Offers what the source saw. The observation belongs to the feed until
     * {@link Source#release(Observation)} hands it back.
     *
     * @param observation what it saw
     */
    public void observed(final O observation) {
        offer(OBSERVED, observation, observation.scope());
    }

    /**
     * Says a thing is gone, for a source that reports its removals rather than sending whole
     * areas. Everything materialized for that id goes with it.
     *
     * @param externalId the id the thing was known by
     */
    public void removed(final CharSequence externalId) {
        offer(REMOVED, externalId.toString(), 0);
    }

    /**
     * Says everything offered for an area up to now is the whole of it, so whatever the feed
     * put there and has not seen this time round is gone. The first such word converges the
     * feed.
     *
     * @param scope the area
     */
    public void complete(final int scope) {
        offer(COMPLETE, null, scope);
    }

    /**
     * Says the source cannot see any more. Nothing is swept until it can again: what it was
     * modelling stays, and the materializer is told so it can say as much in the graph.
     *
     * @param reason why it cannot
     */
    public void unavailable(final Throwable reason) {
        offer(UNAVAILABLE, reason, 0);
    }

    /**
     * Says the source can see again.
     */
    public void available() {
        offer(AVAILABLE, null, 0);
    }

    /**
     * Stops the source and lets go of everything not yet materialized. What the feed has put
     * in the structure stays there.
     */
    public void close() {
        synchronized (intakeLock) {
            if (closing) {
                return;
            }
            closing = true;
        }
        source.stop();
        structure.unwatchDemand(demand);
        loop.detach(this);
        releasePending();
        if (!structure.isClosed()) {
            structure.submit(sourceId, closeTask);
        } else {
            state = FeedState.CLOSED;
        }
    }

    private void offer(final byte kind, final Object payload, final int scope) {
        final boolean wake;
        synchronized (intakeLock) {
            if (closing) {
                releaseIfObservation(kind, payload);
                return;
            }
            filling.add(kind, payload, scope);
            wake = !scheduled;
            scheduled = true;
        }
        if (wake) {
            loop.execute(drainTask);
        }
    }

    private void drainOnLoop() {
        final Batch ready;
        synchronized (intakeLock) {
            if (inFlight) {
                return;                 // the batch in flight will come back for the rest
            }
            scheduled = false;
            if (filling.size == 0) {
                return;
            }
            final Batch swap = draining;
            draining = filling;
            filling = swap;
            inFlight = true;
            ready = draining;
        }
        coalesce(ready);
        loop.ready(this);
    }

    private void coalesce(final Batch batch) {
        for (int i = batch.size - 1; i >= 0; i--) {
            if (batch.kinds[i] != OBSERVED) {
                coalesced.clear();      // only what stands between two words of the source
                continue;
            }
            @SuppressWarnings("unchecked")
            final O observation = (O) batch.payloads[i];
            final CharSequence externalId = observation.externalId();
            if (coalesced.get(externalId) != null) {
                batch.payloads[i] = null;
                source.release(observation);
                continue;
            }
            coalesced.put(externalId, observation);
        }
        coalesced.clear();
    }

    /**
     * The batch handed over is done with: the source is told what failed, and given back what
     * it offered.
     *
     * @param handoverFailure why the handover it went in failed, or null
     */
    void applied(final Throwable handoverFailure) {
        final Throwable batchFailure = applyFailure != null ? applyFailure : handoverFailure;
        applyFailure = null;
        reportFailures(draining, batchFailure);
        releaseAll(draining);
        draining.clear();
        synchronized (intakeLock) {
            inFlight = false;
            scheduled = false;
        }
        drainOnLoop();
    }

    /**
     * @param batch        the batch
     * @param batchFailure why none of it was applied, or null
     */
    @SuppressWarnings("unchecked")
    private void reportFailures(final Batch batch, final Throwable batchFailure) {
        for (int i = 0; i < batch.size; i++) {
            final Throwable failure = batchFailure != null ? batchFailure : batch.failures[i];
            if (failure == null) {
                continue;
            }
            if (batch.kinds[i] != OBSERVED) {
                source.rejected(null, failure);
            } else if (batch.payloads[i] != null) {
                source.rejected((O) batch.payloads[i], failure);
            }
        }
    }

    private void releaseAll(final Batch batch) {
        for (int i = 0; i < batch.size; i++) {
            releaseIfObservation(batch.kinds[i], batch.payloads[i]);
            batch.payloads[i] = null;
        }
    }

    private void releasePending() {
        synchronized (intakeLock) {
            releaseAll(filling);
            filling.clear();
        }
    }

    @SuppressWarnings("unchecked")
    private void releaseIfObservation(final byte kind, final Object payload) {
        if (kind == OBSERVED && payload != null) {
            source.release((O) payload);
        }
    }

    private void demandOnLoop() {
        final Levels told;
        synchronized (demandLock) {
            demandScheduled = false;
            final Levels swap = telling;
            telling = pending;
            pending = swap;
            told = telling;
        }
        for (int i = 0; i < told.size; i++) {
            source.detailLevelChanged(told.ids[i], DetailLevel.of(told.levels[i]));
        }
        told.clear();
    }

    void applyOnStructure() {
        applyFailure = structure.runAs(sourceId, applyTask);
    }

    /**
     * Applies what the source said, word by word: a word that fails is its own business, and
     * the rest go in.
     */
    private void applyBatch() {
        final Batch batch = draining;
        for (int i = 0; i < batch.size; i++) {
            try {
                batch.failures[i] = apply(batch.kinds[i], batch.payloads[i], batch.scopes[i]);
            } catch (final Throwable failure) {
                batch.failures[i] = failure;
            }
        }
    }

    /**
     * @param kind    what the source said
     * @param payload what it said it of
     * @param scope   the area, for a word about one
     * @return why an observation could not be materialized, or null
     */
    private Throwable apply(final byte kind, final Object payload, final int scope) {
        switch (kind) {
            case OBSERVED:
                if (payload != null) {
                    @SuppressWarnings("unchecked")
                    final O observation = (O) payload;
                    return materialize(observation);
                }
                return null;
            case REMOVED:
                forget((CharSequence) payload);
                return null;
            case COMPLETE:
                sweep(scope);
                return null;
            case UNAVAILABLE:
                moveTo(FeedState.STALE, (Throwable) payload);
                return null;
            case AVAILABLE:
                moveTo(converged ? FeedState.CONVERGED : FeedState.LOADING, null);
                return null;
            default:
                return null;
        }
    }

    private Void closeOnStructure() {
        moveTo(FeedState.CLOSED, null);
        return null;
    }

    /**
     * @param observation what the source saw
     * @return why it could not be materialized, or null. What a failed one reached is claimed
     *         for it all the same - it is in the structure, and has to go with the area - and
     *         what it held before is kept, as nothing said it had gone.
     */
    private Throwable materialize(final O observation) {
        final CharSequence externalId = observation.externalId();
        final int scope = observation.scope();
        final Area area = areaOf(scope);
        Entry entry = entries.get(externalId);
        if (entry != null && entry.scope == scope && sameVersion(entry, observation.version())) {
            entry.mark = area.mark;     // unchanged, and so still here
            return null;
        }
        if (entry == null) {
            entry = new Entry(externalId.toString());
            entries.put(entry.externalId, entry);
            area.add(entry);
        } else if (entry.scope != scope) {
            areas.get(entry.scope).remove(entry);
            area.add(entry);
        }
        entry.scope = scope;
        Throwable failure = null;
        emit.beginObservation();
        try {
            materializer.materialize(observation, emit);
        } catch (final Throwable thrown) {
            failure = thrown;
        } finally {
            emit.endObservation();
        }
        reclaim(entry, emit.touchedIds(), emit.touchedCount(), failure == null);
        entry.mark = area.mark;
        rememberVersion(entry, failure == null ? observation.version() : null);
        return failure;
    }

    private void forget(final CharSequence externalId) {
        final Entry entry = entries.get(externalId);
        if (entry == null) {
            return;
        }
        drop(entry);
        areas.get(entry.scope).remove(entry);
        entries.remove(externalId);
    }

    private void sweep(final int scope) {
        if (state == FeedState.STALE) {
            return;                     // unseen is not gone
        }
        final Area area = areaOf(scope);
        for (int i = area.size - 1; i >= 0; i--) {
            final Entry entry = area.entries[i];
            if (entry.mark == area.mark || entry.level == DetailLevel.OFF.ordinal()) {
                continue;               // what we told the source to stop looking at
            }
            drop(entry);
            area.remove(entry);         // the last moves into the hole, and it has been seen
            entries.remove(entry.externalId);
        }
        area.mark++;
        converged = true;
        moveTo(FeedState.CONVERGED, null);
    }

    private void drop(final Entry entry) {
        for (int i = 0; i < entry.idCount; i++) {
            final long objectId = entry.ids[i];
            if (claims.release(objectId)) {
                structure.remove(objectId);
            }
        }
        entry.idCount = 0;
    }

    /**
     * @param entry what one observed id came to
     * @param ids   what materializing it reached this time
     * @param count how many
     * @param whole whether that is all of it now, so what it held and did not reach is let go
     */
    private void reclaim(final Entry entry, final long[] ids, final int count, final boolean whole) {
        final int held = entry.idCount;
        int kept = held;
        for (int i = 0; i < count; i++) {
            if (!holds(entry.ids, held, ids[i])) {
                claims.claim(ids[i]);
                if (!whole) {
                    if (kept == entry.ids.length) {
                        entry.ids = Arrays.copyOf(entry.ids, kept * 2);
                    }
                    entry.ids[kept++] = ids[i];
                }
            }
        }
        if (!whole) {
            entry.idCount = kept;
            return;
        }
        for (int i = 0; i < held; i++) {
            final long objectId = entry.ids[i];
            if (!holds(ids, count, objectId) && claims.release(objectId)) {
                structure.remove(objectId);
            }
        }
        if (entry.ids.length < count) {
            entry.ids = new long[count];
        }
        System.arraycopy(ids, 0, entry.ids, 0, count);
        entry.idCount = count;
    }

    private static boolean holds(final long[] ids, final int count, final long objectId) {
        for (int i = 0; i < count; i++) {
            if (ids[i] == objectId) {
                return true;
            }
        }
        return false;
    }

    private Area areaOf(final int scope) {
        Area area = areas.get(scope);
        if (area == null) {
            area = new Area();
            areas.put(scope, area);
        }
        return area;
    }

    private static boolean sameVersion(final Entry entry, final CharSequence version) {
        return entry.versioned && version != null
                && TextObjectMap.sameText(entry.version, version);
    }

    private static void rememberVersion(final Entry entry, final CharSequence version) {
        if (version == null) {
            entry.versioned = false;
            return;
        }
        entry.version.setLength(0);
        entry.version.append(version);
        entry.versioned = true;
    }

    /**
     * What is wanted of an object has changed. A value this source would no longer supply is
     * not a value anyone should still be shown, so it goes now rather than at the next
     * observation; the source is told on its own thread, and what it does about it is its own
     * business.
     *
     * @param object the object
     * @param level  what is wanted of it now
     */
    private void demandChanged(final StructureObject object, final DetailLevel level) {
        dropUnsupplied(object, level.ordinal());
        final CharSequence externalId = object.externalId();
        final Entry entry = externalId == null ? null : entries.get(externalId);
        if (entry == null) {
            return;                     // not a thing this source speaks about
        }
        entry.level = (byte) level.ordinal();
        final boolean wake;
        synchronized (demandLock) {
            pending.add(externalId, (byte) level.ordinal());
            wake = !demandScheduled;
            demandScheduled = true;
        }
        if (wake) {
            loop.execute(demandTask);
        }
    }

    private void dropUnsupplied(final StructureObject object, final int level) {
        for (int keyId = 0; keyId < supplyThresholds.length; keyId++) {
            if (supplyThresholds[keyId] <= level
                    || object.valueTypeOf(keyId) == ValueType.ABSENT
                    || object.sourceOf(keyId) != sourceId) {
                continue;
            }
            structure.removeProperty(object.id(), keyId);
        }
    }

    private void moveTo(final FeedState next, final Throwable reason) {
        if (state == next) {
            return;
        }
        state = next;
        materializer.stateChanged(next, reason, emit);
    }

    /**
     * What the source has said, in the order it said it. Two of these: the source fills one
     * while the structure works through the other.
     */
    private static final class Batch {
        private byte[] kinds = new byte[32];
        private Object[] payloads = new Object[32];
        private int[] scopes = new int[32];
        private Throwable[] failures = new Throwable[32];
        private int size;

        private void add(final byte kind, final Object payload, final int scope) {
            if (size == kinds.length) {
                kinds = Arrays.copyOf(kinds, size * 2);
                payloads = Arrays.copyOf(payloads, size * 2);
                scopes = Arrays.copyOf(scopes, size * 2);
                failures = Arrays.copyOf(failures, size * 2);
            }
            kinds[size] = kind;
            payloads[size] = payload;
            scopes[size] = scope;
            size++;
        }

        private void clear() {
            Arrays.fill(payloads, 0, size, null);
            Arrays.fill(failures, 0, size, null);
            size = 0;
        }
    }

    /**
     * What one observed id came to: the version last materialized for it, every object that
     * materializing it reached, and how much of it anyone wants - a thing the source was told
     * to stop looking at is not swept for not having been seen.
     */
    private static final class Entry {
        private final String externalId;
        private final StringBuilder version = new StringBuilder();
        private boolean versioned;
        private int scope;
        private int indexInArea;
        private long mark;
        private byte level = (byte) DetailLevel.FINE.ordinal();
        private long[] ids = new long[4];
        private int idCount;

        private Entry(final String externalId) {
            this.externalId = externalId;
        }
    }

    /**
     * What the source still has to be told about demand: the last level per id, since an id
     * asked about twice before the loop wakes has only one answer.
     */
    private static final class Levels {
        private CharSequence[] ids = new CharSequence[8];
        private byte[] levels = new byte[8];
        private final TextObjectMap<Integer> positions = new TextObjectMap<>();
        private int size;

        private void add(final CharSequence externalId, final byte level) {
            final Integer at = positions.get(externalId);
            if (at != null) {
                levels[at] = level;
                return;
            }
            if (size == ids.length) {
                ids = Arrays.copyOf(ids, size * 2);
                levels = Arrays.copyOf(levels, size * 2);
            }
            ids[size] = externalId;
            levels[size] = level;
            positions.put(externalId, size);
            size++;
        }

        private void clear() {
            Arrays.fill(ids, 0, size, null);
            positions.clear();
            size = 0;
        }
    }

    /**
     * An area, which round of it is being observed now, and what it holds: a sweep reads the
     * area, not everything the source has put everywhere.
     */
    private static final class Area {
        private long mark = 1;
        private Entry[] entries = new Entry[8];
        private int size;

        private void add(final Entry entry) {
            if (size == entries.length) {
                entries = Arrays.copyOf(entries, size * 2);
            }
            entry.indexInArea = size;
            entries[size++] = entry;
        }

        private void remove(final Entry entry) {
            final Entry last = entries[--size];
            entries[entry.indexInArea] = last;
            last.indexInArea = entry.indexInArea;
            entries[size] = null;
        }
    }
}
