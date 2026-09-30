package io.github.green4j.sdsm;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * View owns:
 * <ul>
 *   <li>selector and property key filter</li>
 *   <li>cached membership: what the selector matched, and what is delivered</li>
 *   <li>what it owes its subscribers: structural records already written, properties still to
 *       be read at drain time</li>
 *   <li>delivery policy and drain scheduling state</li>
 *   <li>the subscriber list, and the batches travelling to it</li>
 * </ul>
 * A batch travels from the structure's thread to whatever thread delivers it and comes back
 * empty, so a steady stream of changes runs on a fixed set of buffers.
 * <p>
 * A view watches the structure and is not part of it: no selector finds it, and it is made,
 * changed and removed through the structure's view methods.
 */
public final class View extends Watcher {

    private static final int MAX_CONSECUTIVE_SUBSCRIBER_FAILURES = 3;
    private static final int RING_CAPACITY = 64;
    private static final int RING_MASK = RING_CAPACITY - 1;
    private static final Subscription[] NO_SUBSCRIBERS = new Subscription[0];

    /**
     * Where a view is with handing over what it owes. {@code BLOCKED} waits for the delivery
     * thread to make room, which then hands the view back to the structure's thread.
     */
    private enum DrainState {
        IDLE,
        SCHEDULED,
        BLOCKED
    }

    private final long id;
    private final String name;
    private final Structure ownerStructureRef;

    private String selectorText;
    private Selector.Expression selectorExpression;
    private Set<String> propertyKeysOfInterest;
    private int[] keyIdsOfInterest;
    private DeliveryPolicy deliveryPolicy;

    private final LongSet selectorMatched = new LongSet();
    private final LongSet matchedObjectIds = new LongSet();
    private final DirtyProperties dirtyProperties = new DirtyProperties();
    private StructureBatch pending;
    private boolean snapshotOwedToAll;
    private final List<BatchSubscriber> snapshotsOwed = new ArrayList<>(2);

    private volatile Subscription[] subscriptions = NO_SUBSCRIBERS;
    private volatile boolean terminated;

    private long batchSequenceCounter;
    private long lastDrainNanos;
    private final AtomicReference<DrainState> drainState = new AtomicReference<>(DrainState.IDLE);
    private boolean draining;
    private boolean redrain;
    private boolean redrainPromptly;
    private ScheduledFuture<?> scheduledDrain;
    private final Runnable timerFired;
    private final Runnable roomMade;

    private final StructureBatch[] freeRing = new StructureBatch[RING_CAPACITY];
    private final AtomicLong freeWrite = new AtomicLong();
    private final AtomicLong freeRead = new AtomicLong();

    private final StructureBatch[] outRing = new StructureBatch[RING_CAPACITY];
    private final AtomicLong outWrite = new AtomicLong();
    private final AtomicLong outRead = new AtomicLong();
    private final AtomicBoolean deliveryScheduled = new AtomicBoolean();
    private final Runnable deliveryTask = new Runnable() {
        @Override
        public void run() {
            deliverQueued();
        }
    };

    View(final long id,
            final String name,
            final String selectorText,
            final Set<String> propertyKeysOfInterest,
            final DeliveryPolicy deliveryPolicy,
            final Structure ownerStructureRef) {
        this.id = id;
        this.name = name;
        this.ownerStructureRef = ownerStructureRef;
        this.selectorText = selectorText;
        this.selectorExpression = Selector.parse(selectorText, ownerStructureRef.propertyKeys(),
                ownerStructureRef::parentOn);
        this.deliveryPolicy = deliveryPolicy;
        this.lastDrainNanos = System.nanoTime();
        this.timerFired = ownerStructureRef.asOwnTask(this::onTimer);
        this.roomMade = ownerStructureRef.asOwnTask(this::onRoom);
        replacePropertyKeys(propertyKeysOfInterest);
    }

    public long id() {
        return id;
    }

    public String name() {
        return name;
    }

    public String selectorText() {
        return selectorText;
    }

    Selector.Expression selectorExpression() {
        return selectorExpression;
    }

    @Override
    boolean reads(final int keyId) {
        return selectorExpression.reads(keyId);
    }

    @Override
    int reach() {
        return reachOf(selectorExpression);
    }

    Set<String> propertyKeysOfInterest() {
        return propertyKeysOfInterest;
    }

    /**
     * @return ids the selector matches
     */
    LongSet selectorMatched() {
        return selectorMatched;
    }

    /**
     * @return ids delivered to subscribers: what the selector matched, and the ports of the
     *         links among them
     */
    LongSet matchedObjectIds() {
        return matchedObjectIds;
    }

    /**
     * Takes a subscriber on and owes it a snapshot; until the snapshot is on its way, no delta is
     * its to see.
     *
     * @param subscriber the subscriber
     */
    void attach(final BatchSubscriber subscriber) {
        addSubscriber(subscriber);
        restateTo(subscriber);
    }

    void detach(final BatchSubscriber subscriber) {
        removeSubscriber(subscriber);
        snapshotsOwed.remove(subscriber);
    }

    /**
     * @param subscriber whom to send a snapshot built when it goes, ahead of any delta after it
     */
    void restateTo(final BatchSubscriber subscriber) {
        snapshotsOwed.add(subscriber);
    }

    /**
     * Owes every subscriber a snapshot instead of the deltas it was owed: what the view holds, or
     * which of its properties it delivers, has been redefined.
     */
    void restateToAll() {
        snapshotOwedToAll = true;
    }

    private synchronized void addSubscriber(final BatchSubscriber subscriber) {
        if (indexOf(subscriptions, subscriber) >= 0) {
            return;
        }
        final Subscription[] current = subscriptions;
        final Subscription[] grown = new Subscription[current.length + 1];
        System.arraycopy(current, 0, grown, 0, current.length);
        grown[current.length] = new Subscription(subscriber);
        subscriptions = grown;
    }

    synchronized void removeSubscriber(final BatchSubscriber subscriber) {
        final Subscription[] current = subscriptions;
        final int at = indexOf(current, subscriber);
        if (at < 0) {
            return;
        }
        final Subscription[] shrunk = new Subscription[current.length - 1];
        System.arraycopy(current, 0, shrunk, 0, at);
        System.arraycopy(current, at + 1, shrunk, at, current.length - at - 1);
        subscriptions = shrunk;
    }

    private static int indexOf(final Subscription[] list, final BatchSubscriber subscriber) {
        for (int i = 0; i < list.length; i++) {
            if (list[i].subscriber == subscriber) {
                return i;
            }
        }
        return -1;
    }

    /**
     * @param newSelectorText the selector; a selector that does not parse changes nothing
     */
    void replaceSelector(final String newSelectorText) {
        this.selectorExpression =
                Selector.parse(newSelectorText, ownerStructureRef.propertyKeys(),
                        ownerStructureRef::parentOn);
        this.selectorText = newSelectorText;
    }

    void replacePropertyKeys(final Set<String> newKeys) {
        if (newKeys == null || newKeys.isEmpty()) {
            propertyKeysOfInterest = null;
            keyIdsOfInterest = null;
            return;
        }
        final Set<String> keys = new LinkedHashSet<>(newKeys);
        final int[] keyIds = new int[keys.size()];
        int at = 0;
        for (final String key : keys) {
            keyIds[at++] = ownerStructureRef.propertyKeys().idOf(key);
        }
        propertyKeysOfInterest = keys;
        keyIdsOfInterest = keyIds;
    }

    void replaceDeliveryPolicy(final DeliveryPolicy newPolicy) {
        stopTimer();
        deliveryPolicy = newPolicy;
    }

    /**
     * A key filter says which of the properties an object reports are worth delivering. What
     * the object is - its name, its type, the node a port is on, the ports a link joins - is
     * not reportage, and a view that filters keys still gets all of it.
     *
     * @param keyId the property
     * @return whether this view delivers it
     */
    boolean isInterestedInKey(final int keyId) {
        if (keyIdsOfInterest == null || PropertyKeys.isReserved(keyId)) {
            return true;
        }
        for (int i = 0; i < keyIdsOfInterest.length; i++) {
            if (keyIdsOfInterest[i] == keyId) {
                return true;
            }
        }
        return false;
    }

    boolean hasPending() {
        return dirtyProperties.size() > 0 || (pending != null && pending.size() > 0);
    }

    private void clearPending() {
        dirtyProperties.clear();
        if (pending != null) {
            pending.buffer().clear();
        }
    }

    void enqueueStructural(final ChangeKind changeKind, final StructureObject target) {
        pendingBatch().buffer().appendStructural(changeKind, target.id(), target.kind());
    }

    void enqueueMembership(final ChangeKind changeKind,
                           final StructureObject member,
                           final long parentId) {
        pendingBatch().buffer()
                .appendMembership(changeKind, member.id(), member.kind(), parentId);
    }

    void markDirty(final long objectId, final int keyId) {
        dirtyProperties.mark(objectId, keyId);
    }

    void markEveryPropertyDirty(final StructureObject object) {
        final int count = object.propertyCount();
        for (int i = 0; i < count; i++) {
            final int keyId = object.propertyKeyIdAt(i);
            if (isInterestedInKey(keyId) && object.valueTypeOf(keyId) != ValueType.ABSENT) {
                dirtyProperties.mark(object.id(), keyId);
            }
        }
    }

    /**
     * Terminates all subscribers, notifying each of the reason, and drops everything this view
     * was holding for them. Called when the view leaves the structure.
     *
     * @param reason termination reason passed to subscribers
     */
    synchronized void terminateSubscribers(final Throwable reason) {
        terminated = true;
        stopTimer();
        clearPending();
        snapshotOwedToAll = false;
        snapshotsOwed.clear();
        clearMembership();
        final Subscription[] terminated = subscriptions;
        subscriptions = NO_SUBSCRIBERS;
        for (int i = 0; i < terminated.length; i++) {
            try {
                terminated[i].subscriber.onError(reason);
            } catch (final Throwable ignored) {
                // Best-effort notification
            }
        }
    }

    /**
     * @return what the view owes its subscribers, as a batch the caller now owns; null if what
     *         changed has left the view since, which costs no number
     */
    private StructureBatch takePendingBatch() {
        final StructureBatch batch = pendingBatch();
        pending = null;
        final ChangeBuffer buffer = batch.buffer();
        for (int i = 0; i < dirtyProperties.size(); i++) {
            final long objectId = dirtyProperties.objectIdAt(i);
            if (!matchedObjectIds.contains(objectId)) {
                continue;
            }
            final StructureObject object = ownerStructureRef.lookupOrNull(objectId);
            if (object != null) {
                appendValue(buffer, object, dirtyProperties.keyIdAt(i));
            }
        }
        dirtyProperties.clear();
        if (batch.isEmpty()) {
            pending = batch;
            return null;
        }
        batchSequenceCounter++;
        batch.stamp(batchSequenceCounter, ownerStructureRef.version(), System.nanoTime(), false);
        return batch;
    }

    /**
     * Says what is there before it says anything about it: a record naming two objects -
     * membership, and the ports of a link - is only readable by a receiver that has both,
     * and the order the ids happen to sit in is nobody's contract.
     *
     * @param target the only subscriber to send it to, or null for every subscriber
     * @return a complete statement of what the view holds now, built from current state
     */
    private StructureBatch snapshotFor(final BatchSubscriber target) {
        final StructureBatch batch = acquireBatch();
        final ChangeBuffer buffer = batch.buffer();
        for (int i = 0; i < matchedObjectIds.size(); i++) {
            final StructureObject object =
                    ownerStructureRef.lookupOrNull(matchedObjectIds.valueAt(i));
            if (object != null) {
                buffer.appendStructural(ChangeKind.ADDED, object.id(), object.kind());
            }
        }
        for (int i = 0; i < matchedObjectIds.size(); i++) {
            final StructureObject object =
                    ownerStructureRef.lookupOrNull(matchedObjectIds.valueAt(i));
            if (object == null) {
                continue;
            }
            final int count = object.propertyCount();
            for (int k = 0; k < count; k++) {
                final int keyId = object.propertyKeyIdAt(k);
                if (isInterestedInKey(keyId) && object.valueTypeOf(keyId) != ValueType.ABSENT) {
                    appendValue(buffer, object, keyId);
                }
            }
            appendMemberships(buffer, object);
        }
        batch.stamp(batchSequenceCounter, ownerStructureRef.version(), System.nanoTime(), true);
        batch.targetOnly(target, target != null && indexOf(subscriptions, target) >= 0);
        final Subscription[] current = subscriptions;
        for (int i = 0; i < current.length; i++) {
            if (target == null || current[i].subscriber == target) {
                current[i].since = batchSequenceCounter;
            }
        }
        return batch;
    }

    /**
     * Membership is stated once per pair, from the member's side, and only where both ends
     * are delivered: a parent this view does not hold is not somewhere anything can be shown.
     *
     * @param buffer buffer to append to
     * @param object the member
     */
    private void appendMemberships(final ChangeBuffer buffer, final StructureObject object) {
        final LongSet parentIds = ownerStructureRef.parentsOf(object.id());
        if (parentIds == null) {
            return;
        }
        for (int i = 0; i < parentIds.size(); i++) {
            final long parentId = parentIds.valueAt(i);
            if (matchedObjectIds.contains(parentId)) {
                buffer.appendMembership(
                        ChangeKind.CONTAINED, object.id(), object.kind(), parentId);
            }
        }
    }

    private void appendValue(final ChangeBuffer buffer,
                             final StructureObject object,
                             final int keyId) {
        final ValueType valueType = object.valueTypeOf(keyId);
        final String keyName = ownerStructureRef.propertyKeys().nameOf(keyId);
        switch (valueType) {
            case LONG:
                buffer.appendProperty(object.id(), object.kind(), keyId, keyName,
                        valueType, object.longValueOf(keyId), null);
                break;
            case DOUBLE:
                buffer.appendProperty(object.id(), object.kind(), keyId, keyName,
                        valueType, Double.doubleToRawLongBits(object.doubleValueOf(keyId)), null);
                break;
            case BOOLEAN:
                buffer.appendProperty(object.id(), object.kind(), keyId, keyName,
                        valueType, object.booleanValueOf(keyId) ? 1L : 0L, null);
                break;
            case TEXT:
                buffer.appendProperty(object.id(), object.kind(), keyId, keyName,
                        valueType, 0L, object.textValueOf(keyId));
                break;
            default:
                buffer.appendProperty(object.id(), object.kind(), keyId, keyName,
                        ValueType.ABSENT, 0L, null);
                break;
        }
    }

    private StructureBatch pendingBatch() {
        if (pending == null) {
            pending = acquireBatch();
        }
        return pending;
    }

    /**
     * @return whether it owes anything: deltas, or a snapshot to someone
     */
    boolean owes() {
        return snapshotOwedToAll || !snapshotsOwed.isEmpty() || hasPending();
    }

    /**
     * Hands over what the view owes, as far as the delivery ring has room: the deltas once the
     * delivery interval has passed, or at once when asked to be prompt or when a snapshot is owed
     * behind them; then the snapshots owed. What does not fit waits in place, still coalescing,
     * until the delivery thread has made room. Runs on the structure's thread.
     *
     * <p>
     * A subscriber called here that changes the structure asks for a drain before the batch it
     * is being handed has reached everyone; that drain waits for this one to finish, so no
     * subscriber sees a batch ahead of the one before it.
     *
     * @param promptly whether to hand the deltas over without waiting for the interval
     */
    void drain(final boolean promptly) {
        if (draining) {
            redrain = true;
            redrainPromptly |= promptly;
            return;
        }
        draining = true;
        try {
            boolean prompt = promptly;
            do {
                redrain = false;
                redrainPromptly = false;
                drainOnce(prompt);
                prompt = redrainPromptly;
            } while (redrain);
        } finally {
            draining = false;
        }
    }

    private void drainOnce(final boolean promptly) {
        if (drainState.get() == DrainState.BLOCKED) {
            return;                     // the delivery thread hands the view back when there is room
        }
        if (snapshotOwedToAll) {
            clearPending();
        }
        if (hasPending()) {
            final long wait = lastDrainNanos + deliveryPolicy.minIntervalNanos() - System.nanoTime();
            if (wait > 0 && !promptly && snapshotsOwed.isEmpty()) {
                if (drainState.get() == DrainState.IDLE) {
                    startTimer(wait);
                }
                return;
            }
            if (!roomOrBlock()) {
                return;
            }
            final StructureBatch deltas = takePendingBatch();
            if (deltas != null) {
                enqueue(deltas);
            }
        }
        if (snapshotOwedToAll) {
            if (!roomOrBlock()) {
                return;
            }
            snapshotOwedToAll = false;
            enqueue(snapshotFor(null));
        }
        while (!snapshotsOwed.isEmpty()) {
            if (!roomOrBlock()) {
                return;
            }
            enqueue(snapshotFor(snapshotsOwed.remove(0)));
        }
    }

    /**
     * @return whether a batch can go now; if not, the view is blocked until the delivery thread
     *         has made room
     */
    private boolean roomOrBlock() {
        if (hasRoom()) {
            return true;
        }
        stopTimer();
        drainState.set(DrainState.BLOCKED);
        // the delivery thread may have made room between the two looks without seeing BLOCKED
        return hasRoom() && drainState.compareAndSet(DrainState.BLOCKED, DrainState.IDLE);
    }

    private boolean hasRoom() {
        return ownerStructureRef.deliveryExecutor() == null
                || outWrite.get() - outRead.get() < RING_CAPACITY;
    }

    private void startTimer(final long delayNanos) {
        drainState.set(DrainState.SCHEDULED);
        scheduledDrain = ownerStructureRef.after(timerFired, delayNanos);
    }

    private void stopTimer() {
        if (drainState.get() == DrainState.SCHEDULED) {
            scheduledDrain.cancel(false);
            scheduledDrain = null;
            drainState.set(DrainState.IDLE);
        }
    }

    private void onTimer() {
        if (drainState.get() == DrainState.SCHEDULED) {
            scheduledDrain = null;
            drainState.set(DrainState.IDLE);
            drain(false);
        }
    }

    private void onRoom() {
        drain(false);
    }

    /**
     * @param batch a batch whose ownership passes on
     */
    private void enqueue(final StructureBatch batch) {
        lastDrainNanos = System.nanoTime();
        final Executor executor = ownerStructureRef.deliveryExecutor();
        if (executor == null) {
            deliverNow(batch);
            recycle(batch);
            return;
        }
        final long writeAt = outWrite.get();
        outRing[(int) (writeAt & RING_MASK)] = batch;
        outWrite.lazySet(writeAt + 1);
        if (deliveryScheduled.compareAndSet(false, true)) {
            executor.execute(deliveryTask);
        }
    }

    private void deliverQueued() {
        while (true) {
            long readAt = outRead.get();
            while (readAt < outWrite.get()) {
                final int slot = (int) (readAt & RING_MASK);
                final StructureBatch batch = outRing[slot];
                outRing[slot] = null;
                readAt++;
                outRead.set(readAt);    // a volatile write: roomOrBlock reads it after BLOCKED
                deliverNow(batch);
                recycle(batch);
            }
            if (drainState.get() == DrainState.BLOCKED
                    && drainState.compareAndSet(DrainState.BLOCKED, DrainState.IDLE)) {
                ownerStructureRef.execute(roomMade);
            }
            deliveryScheduled.set(false);
            if (outRead.get() >= outWrite.get()) {
                return;
            }
            if (!deliveryScheduled.compareAndSet(false, true)) {
                return;
            }
        }
    }

    /**
     * Delivers nothing once the view has gone, and a snapshot for one subscriber only while it is
     * still one: either may have happened while the batch was on its way.
     *
     * @param batch what to deliver
     */
    private void deliverNow(final StructureBatch batch) {
        if (terminated) {
            return;
        }
        final BatchSubscriber only = batch.target();
        if (only != null) {
            if (batch.targetSubscribed() && indexOf(subscriptions, only) < 0) {
                return;
            }
            try {
                only.onBatch(batch);
            } catch (final Throwable failure) {
                reportFailure(only, failure);
            }
            return;
        }
        final long sequence = batch.batchSequenceNumber();
        final boolean snapshot = batch.isInitialSnapshot();
        final Subscription[] current = subscriptions;
        for (int i = 0; i < current.length; i++) {
            final Subscription subscription = current[i];
            final long since = subscription.since;
            if (since > sequence || since == sequence && !snapshot) {
                continue;               // before its last snapshot, or before its first
            }
            try {
                subscription.subscriber.onBatch(batch);
                subscription.failures = 0;
            } catch (final Throwable failure) {
                subscription.failures++;
                if (subscription.failures >= MAX_CONSECUTIVE_SUBSCRIBER_FAILURES) {
                    removeSubscriber(subscription.subscriber);
                    reportFailure(subscription.subscriber, new RuntimeException(
                            "Subscriber evicted after " + subscription.failures
                                    + " consecutive failures", failure));
                } else {
                    reportFailure(subscription.subscriber, failure);
                }
            }
        }
    }

    private static void reportFailure(final BatchSubscriber subscriber, final Throwable failure) {
        try {
            subscriber.onError(failure);
        } catch (final Throwable ignored) {
            // Best-effort notification
        }
    }

    private StructureBatch acquireBatch() {
        final long readAt = freeRead.get();
        if (readAt < freeWrite.get()) {
            final int slot = (int) (readAt & RING_MASK);
            final StructureBatch reused = freeRing[slot];
            freeRing[slot] = null;
            freeRead.lazySet(readAt + 1);
            return reused;
        }
        return new StructureBatch();
    }

    /**
     * Returns an emptied batch for reuse. Called only by whoever just delivered it, which is one
     * thread at a time - the free list has a single producer, and a batch dropped on any other
     * path is simply let go.
     *
     * @param batch batch that has been delivered
     */
    private void recycle(final StructureBatch batch) {
        batch.reset();
        final long writeAt = freeWrite.get();
        if (writeAt - freeRead.get() >= RING_CAPACITY) {
            return;
        }
        freeRing[(int) (writeAt & RING_MASK)] = batch;
        freeWrite.lazySet(writeAt + 1);
    }

    void clearMembership() {
        selectorMatched.clear();
        matchedObjectIds.clear();
    }

    void addMember(final long objectId) {
        matchedObjectIds.add(objectId);
    }

    void removeMember(final long objectId) {
        matchedObjectIds.remove(objectId);
    }

    private static final class Subscription {
        private final BatchSubscriber subscriber;
        private int failures;
        /** The number of its last snapshot; until the first is on its way, past every number. */
        private volatile long since = Long.MAX_VALUE;

        Subscription(final BatchSubscriber subscriber) {
            this.subscriber = subscriber;
        }
    }
}
