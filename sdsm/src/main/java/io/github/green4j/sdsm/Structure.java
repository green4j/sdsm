package io.github.green4j.sdsm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Thread-safety contract:
 * <p>
 * - The structure - its objects, their relations and properties - is changed on its own worker
 * thread, inside a task reached through {@link #run(Runnable)} or {@link #submit(Supplier)}. A
 * task is the transaction: everything it changes reaches subscribers as one batch.
 * - Reads, views, subscriptions and interest can be asked from any thread and answer with a
 * future; asked inside a task, they are answered there and then.
 * - Subscriber callbacks run on the worker thread, or on the executor given to
 * {@link #withDeliveryExecutor(Executor)} when there is one.
 * - A {@link StructureObject} is read outside the worker thread only for what never changes.
 */
public final class Structure {
    /**
     * Which structure the current thread is executing a task of, so a task reaching another
     * structure is not taken for a nested call. Put back to what it was rather than removed:
     * removing it makes the next {@code get()} build the map entry again, and on the worker
     * thread that is an allocation per task.
     */
    private static final ThreadLocal<Structure> CURRENT_EXECUTING_STRUCTURE = new ThreadLocal<>();

    private static final int STATE_RUNNING = 0;
    private static final int STATE_CLOSING = 1;
    private static final int STATE_CLOSED = 2;

    private static final View[] NO_VIEWS = new View[0];
    private static final DemandListener[] NO_DEMAND_LISTENERS = new DemandListener[0];

    /**
     * Source id of a write nobody claims. Ids above it are the caller's own numbering; the
     * structure stores them and compares them, and never interprets them.
     */
    public static final int NO_SOURCE = 0;

    private static final int INHERIT_SOURCE = -1;

    // what a membership re-evaluation is asked for when more than one property changed
    private static final int ANY_KEY = -1;

    private final ExecutorService worker;
    private final ScheduledExecutorService scheduler;

    private final PropertyKeys propertyKeys = PropertyKeys.OF_PROCESS;

    private final LongObjectMap<StructureObject> objectsById = new LongObjectMap<>();
    private final TextObjectMap<StructureObject> objectsByExternalId = new TextObjectMap<>();

    private final Domains domains = Domains.OF_PROCESS;

    /**
     * Address index: one address -> the ids of every port declaring it, either side.
     */
    private final TextObjectMap<LongSet> portIdsByAddress = new TextObjectMap<>();

    /**
     * Route index: one domain -> the ids of the ports looking for an address in it, the side
     * a route is opened for.
     */
    private final LongObjectMap<LongSet> requiringPortIdsByDomain = new LongObjectMap<>();

    /**
     * The cross-domain pairs a link may span, packed requiring domain over providing
     * domain. A domain always reaches itself and what belongs to no domain, so only crossings
     * between named ones are held here.
     */
    private final LongSet allowedRoutes = new LongSet();
    private volatile View[] views = NO_VIEWS;
    private final LongObjectMap<View> viewsById = new LongObjectMap<>();
    private volatile Grouping[] groupings = new Grouping[0];
    private final LongObjectMap<Grouping> groupingsById = new LongObjectMap<>();
    private final TextObjectMap<Grouping> groupingsByName = new TextObjectMap<>();
    private final LongObjectMap<Grouping> groupingOfNode = new LongObjectMap<>();
    private final StringBuilder groupKey = new StringBuilder(64);

    /**
     * What each client wants, and who is told when that changes. No interest at all means
     * nobody has spoken, which is not the same as nobody wanting anything: everything is
     * wanted whole until a client says otherwise.
     */
    private final TextObjectMap<Interest> interests = new TextObjectMap<>();
    private volatile DemandListener[] demandListeners = NO_DEMAND_LISTENERS;

    /**
     * Link indexes for O(1) cascade removal and retargeting.
     * inputId  -> set of linkIds where link.toInput  == input
     * outputId -> set of linkIds where link.fromOutput == output
     */
    private final LongObjectMap<LongSet> linksByToInputId = new LongObjectMap<>();
    private final LongObjectMap<LongSet> linksByFromOutputId = new LongObjectMap<>();

    /**
     * Child id -> the ids of the nodes holding it, one per axis.
     */
    private final LongObjectMap<LongSet> parentsByChild = new LongObjectMap<>();
    private final LongSet ancestorsSeen = new LongSet();
    private long[] ancestorsToVisit = new long[16];

    private final AtomicLong versionCounter = new AtomicLong(0L);
    private final AtomicLong globalIdCounter = new AtomicLong(0L);

    private volatile Executor deliveryExecutor;

    /**
     * How many derivations read each key, so a write nobody folds costs one look.
     */
    private int[] foldedKeys = new int[0];
    private final Derivation.Sum leafWas = new Derivation.Sum();
    private final Derivation.Sum leafNow = new Derivation.Sum();
    private final Derivation.Sum summand = new Derivation.Sum();
    private final Derivation.Sum nothing = new Derivation.Sum();
    private final Derivation.Sum derivedWas = new Derivation.Sum();
    private final Derivation.Sum derivedNow = new Derivation.Sum();

    /**
     * How many derivations read each key of their own object.
     */
    private int[] ownFoldedKeys = new int[0];

    /**
     * How many observations say each object is there, whichever feed, loop or group made them:
     * the region both clusters sit in outlives the first of them to stop saying it.
     */
    private final LongCounts claims = new LongCounts();

    private int taskDepth;
    private int currentSourceId = NO_SOURCE;

    /**
     * Tracks outstanding futures so they can be completed on shutdown.
     */
    private final Set<CompletableFuture<?>> outstandingFutures =
            ConcurrentHashMap.newKeySet();

    /**
     * Atomic lifecycle state machine.
     * Transitions: RUNNING -> CLOSING -> CLOSED (one-way only).
     */
    private final AtomicInteger lifecycleState = new AtomicInteger(STATE_RUNNING);

    private volatile Runnable onCloseListener;

    Structure(final ExecutorService worker,
              final ScheduledExecutorService scheduler) {
        this.worker = worker;
        this.scheduler = scheduler;
    }

    /**
     * @return the naming dictionary: property names in, key ids out, the same for every structure
     */
    public PropertyKeys propertyKeys() {
        return propertyKeys;
    }

    /**
     * @return the domain dictionary: domain names in, ids out, the same for every structure
     */
    public Domains domains() {
        return domains;
    }

    /**
     * Hands delivery to an executor of the caller's own. SDSM runs no delivery threads: without
     * this, subscribers are called on the structure's thread and a slow one holds up the model.
     * Set it before the first subscriber attaches.
     *
     * @param executor executor that will run subscriber callbacks, or null for inline delivery
     * @return this structure
     */
    public Structure withDeliveryExecutor(final Executor executor) {
        this.deliveryExecutor = executor;
        return this;
    }

    Executor deliveryExecutor() {
        return deliveryExecutor;
    }

    /**
     * @param childId an object
     * @return the parents directly holding it, or null if it is in none
     */
    LongSet parentsOf(final long childId) {
        return parentsByChild.get(childId);
    }

    private void indexMemberAdd(final long childId, final long parentId) {
        addToIndex(parentsByChild, childId, parentId);
    }

    private void indexMemberRemove(final long childId, final long parentId) {
        removeFromIndex(parentsByChild, childId, parentId);
    }

    /**
     * Drops a parent being removed from the reverse index of each of its direct members
     * and tells the views their membership changed. Runs before the parent leaves the index.
     *
     * @param parent the parent being removed
     */
    private void releaseChildrenOf(final Node parent) {
        final long[] childIds = parent.childIds();
        for (int i = 0; i < childIds.length; i++) {
            indexMemberRemove(childIds[i], parent.id());
            emitLeftIfPresent(childIds[i], parent);
        }
    }

    /**
     * An object is a direct member of at most one parent per axis, so a second parent on an
     * axis is a modelling error rather than an alternative placement.
     *
     * @param childId  object being placed
     * @param parentId parent it is being placed into
     * @param axis     the axis
     */
    private void rejectSecondParentOnAxis(final long childId, final long parentId, final String axis) {
        final LongSet parentIds = parentsByChild.get(childId);
        if (parentIds == null) {
            return;
        }
        for (int i = 0; i < parentIds.size(); i++) {
            final long candidateId = parentIds.valueAt(i);
            if (candidateId == parentId) {
                continue;
            }
            final StructureObject candidate = objectsById.get(candidateId);
            if (candidate instanceof Node && axis.equals(((Node) candidate).axis())) {
                throw new IllegalArgumentException(
                        "Object '" + childId + "' is already under '" + candidateId
                                + "' on axis '" + axis + "'");
            }
        }
    }

    long version() {
        return versionCounter.get();
    }

    LongCounts claims() {
        return claims;
    }

    StructureObject lookupOrNull(final long objectId) {
        return objectsById.get(objectId);
    }

    /**
     * @param listener told once, when the structure starts closing
     */
    void setOnCloseListener(final Runnable listener) {
        this.onCloseListener = listener;
    }

    private void notifyCloseListener() {
        final Runnable listener = onCloseListener;
        if (listener != null) {
            try {
                listener.run();
            } catch (final Throwable ignored) {
            }
        }
    }

    /**
     * Closes this structure. New submissions are rejected immediately; admitted
     * tasks already queued on the worker are allowed to finish before a final sweep.
     */
    public void close() {
        if (!lifecycleState.compareAndSet(STATE_RUNNING, STATE_CLOSING)) {
            return;
        }

        notifyCloseListener();

        final RejectedExecutionException shutdownException =
                new RejectedExecutionException("Structure has been closed");

        try {
            worker.execute(new Runnable() {
                @Override
                public void run() {
                    terminateAllViews(shutdownException);
                    clearStateForGc();
                    lifecycleState.set(STATE_CLOSED);
                    completeAllOutstanding(shutdownException);
                }
            });
        } catch (final RejectedExecutionException rejected) {
            terminateAllViews(shutdownException);
            clearStateForGc();
            lifecycleState.set(STATE_CLOSED);
            completeAllOutstanding(shutdownException);
        }
    }

    private void terminateAllViews(final Throwable reason) {
        final View[] snapshot = views;
        for (int i = 0; i < snapshot.length; i++) {
            snapshot[i].terminateSubscribers(reason);
        }
    }

    private void clearStateForGc() {
        views = NO_VIEWS;
        viewsById.clear();
        groupings = new Grouping[0];
        groupingsById.clear();
        groupingsByName.clear();
        groupingOfNode.clear();
        interests.clear();
        objectsById.clear();
        objectsByExternalId.clear();
        linksByToInputId.clear();
        linksByFromOutputId.clear();
        parentsByChild.clear();
        portIdsByAddress.clear();
        requiringPortIdsByDomain.clear();
        allowedRoutes.clear();
        foldedKeys = new int[0];
        ownFoldedKeys = new int[0];
        claims.clear();
    }

    private void addView(final View view) {
        final View[] current = views;
        final View[] grown = new View[current.length + 1];
        System.arraycopy(current, 0, grown, 0, current.length);
        grown[current.length] = view;
        views = grown;
        viewsById.put(view.id(), view);
    }

    private void removeView(final View view) {
        final View[] current = views;
        int at = -1;
        for (int i = 0; i < current.length; i++) {
            if (current[i] == view) {
                at = i;
                break;
            }
        }
        if (at < 0) {
            return;
        }
        final View[] shrunk = new View[current.length - 1];
        System.arraycopy(current, 0, shrunk, 0, at);
        System.arraycopy(current, at + 1, shrunk, at, current.length - at - 1);
        views = shrunk;
        viewsById.remove(view.id());
    }

    private View requireView(final long viewId) {
        final View view = viewsById.get(viewId);
        if (view == null) {
            throw new NoSuchElementException("No such view: " + viewId);
        }
        return view;
    }

    /**
     * Fails every task still waiting: the runtime calls it once its threads have stopped.
     *
     * @param cause the rejection to complete every outstanding future with
     */
    void completeAllOutstanding(final RejectedExecutionException cause) {
        while (!outstandingFutures.isEmpty()) {
            final Iterator<CompletableFuture<?>> it = outstandingFutures.iterator();
            while (it.hasNext()) {
                final CompletableFuture<?> future = it.next();
                it.remove();            // only what is failed here: one added meanwhile stays
                future.completeExceptionally(cause);
            }
        }
    }

    public boolean isClosed() {
        return lifecycleState.get() != STATE_RUNNING;
    }

    private boolean isOnOwnWorkerThread() {
        return CURRENT_EXECUTING_STRUCTURE.get() == this;
    }

    private void requireWorkerThread() {
        if (!isOnOwnWorkerThread()) {
            throw new IllegalStateException(
                    "The structure is read on its own thread; "
                            + "reach it through run(...) or submit(...)");
        }
    }

    /**
     * A subscriber or a timer runs on the worker thread outside any task, and what it changed
     * there would wait for some later task to be delivered.
     */
    private void requireTask() {
        if (!isOnOwnWorkerThread() || taskDepth == 0) {
            throw new IllegalStateException(
                    "The structure is changed inside a task; "
                            + "reach it through run(...) or submit(...)");
        }
    }

    public <ResultType> CompletableFuture<ResultType> submit(final Supplier<ResultType> task) {
        return submitScoped(INHERIT_SOURCE, task);
    }

    /**
     * Runs a task attributing every property it writes to {@code sourceId}. The attribution
     * holds for the task and for whatever it calls, and is restored when the task returns.
     *
     * @param <ResultType> result of the task
     * @param sourceId     the caller's own number for the writer, or {@link #NO_SOURCE}
     * @param task         the task to run on the worker
     * @return future completed with the task's result
     */
    public <ResultType> CompletableFuture<ResultType> submit(final int sourceId,
                                                             final Supplier<ResultType> task) {
        if (sourceId < NO_SOURCE) {
            throw new IllegalArgumentException("sourceId must not be negative: " + sourceId);
        }
        return submitScoped(sourceId, task);
    }

    private <ResultType> CompletableFuture<ResultType> submitScoped(
            final int sourceId,
            final Supplier<ResultType> task) {
        if (isOnOwnWorkerThread()) {
            return runNested(sourceId, task);
        }

        final CompletableFuture<ResultType> futureResult = new CompletableFuture<>();

        if (lifecycleState.get() != STATE_RUNNING) {
            futureResult.completeExceptionally(
                    new RejectedExecutionException("Structure is closed"));
            return futureResult;
        }

        outstandingFutures.add(futureResult);

        final Runnable wrappedTask = new Runnable() {
            @Override
            public void run() {
                final Structure previous = CURRENT_EXECUTING_STRUCTURE.get();
                CURRENT_EXECUTING_STRUCTURE.set(Structure.this);
                final int previousSource = enterTask(sourceId);
                ResultType value = null;
                Throwable failure = null;
                try {
                    if (lifecycleState.get() == STATE_CLOSED) {
                        failure = new RejectedExecutionException(
                                "Structure closed before task execution");
                    } else {
                        value = task.get();
                    }
                } catch (final Throwable thrown) {
                    failure = thrown;
                } finally {
                    try {
                        leaveTask(previousSource);
                    } catch (final Throwable ignored) {
                    } finally {
                        CURRENT_EXECUTING_STRUCTURE.set(previous);
                        outstandingFutures.remove(futureResult);
                    }
                }
                complete(futureResult, value, failure);
            }
        };

        try {
            worker.execute(wrappedTask);
        } catch (final RejectedExecutionException rejection) {
            outstandingFutures.remove(futureResult);
            futureResult.completeExceptionally(rejection);
        }
        return futureResult;
    }

    /**
     * Opens a way of handing work over that costs nothing per handover: the wrapper the worker
     * runs is this one object, made once and used again. Meant for a caller with one piece of
     * work outstanding at a time and nothing to wait on - a loop thread handing over every round -
     * where a future, the stage that reads it and a wrapper are three allocations a round that
     * nobody ever looks at.
     *
     * @param sourceId   the caller's own number for the writer
     * @param work       what to do on the worker thread
     * @param completion told when the work is over, or why it will not be done
     * @return the handover to use for every piece of work from that caller
     */
    Handover handover(final int sourceId, final Runnable work, final Completion completion) {
        return new Handover(sourceId, work, completion);
    }

    /**
     * Runs work already on the worker thread under a source of its own, inside the task that
     * reached it: one handover can carry the writes of several sources and attribute each.
     *
     * @param sourceId source to attribute writes to
     * @param work     what to do
     * @return what it threw, or null
     */
    Throwable runAs(final int sourceId, final Runnable work) {
        requireTask();
        return runHere(sourceId, work);
    }

    /**
     * @param sourceId source to attribute writes to
     * @param work     what to do
     * @return what it threw, or null
     */
    private Throwable runHere(final int sourceId, final Runnable work) {
        final int previousSource = enterTask(sourceId);
        Throwable failure = null;
        try {
            work.run();
        } catch (final Throwable thrown) {
            failure = thrown;
        } finally {
            try {
                leaveTask(previousSource);
            } catch (final Throwable ignored) {
            }
        }
        return failure;
    }

    /**
     * One caller's way of handing work to the worker thread: the scoping and the callback in
     * one object, used again for every piece of work.
     * <p>
     * One piece at a time, and that is the caller's to keep: {@link #ingest()} must not be
     * called again until the completion has been told about the last one.
     */
    final class Handover implements Runnable {
        private final int sourceId;
        private final Runnable work;
        private final Completion completion;

        private Handover(final int sourceId,
                         final Runnable work,
                         final Completion completion) {
            this.sourceId = sourceId;
            this.work = work;
            this.completion = completion;
        }

        /**
         * Hands the work over, or says at once why it will not be done.
         */
        void ingest() {
            if (isOnOwnWorkerThread()) {
                completion.completed(runHere(sourceId, work));
                return;
            }
            if (lifecycleState.get() != STATE_RUNNING) {
                completion.completed(new RejectedExecutionException("Structure is closed"));
                return;
            }
            try {
                worker.execute(this);
            } catch (final RejectedExecutionException rejection) {
                completion.completed(rejection);
            }
        }

        @Override
        public void run() {
            final Structure previous = CURRENT_EXECUTING_STRUCTURE.get();
            CURRENT_EXECUTING_STRUCTURE.set(Structure.this);
            Throwable failure = null;
            try {
                if (lifecycleState.get() == STATE_CLOSED) {
                    failure = new RejectedExecutionException(
                            "Structure closed before task execution");
                } else {
                    failure = runHere(sourceId, work);
                }
            } finally {
                CURRENT_EXECUTING_STRUCTURE.set(previous);
            }
            completion.completed(failure);
        }
    }

    /**
     * Runs a task that is already on the worker thread, there and then. The outermost task is
     * the transaction boundary: what a nested call mutates belongs to the batch of the task
     * that reached it, so the drain waits for the outermost one to finish.
     *
     * @param <ResultType> result of the task
     * @param sourceId     source to attribute writes to, or {@code INHERIT_SOURCE}
     * @param task         the task to run
     * @return future already completed with the task's result or its failure
     */
    private <ResultType> CompletableFuture<ResultType> runNested(final int sourceId,
                                                                 final Supplier<ResultType> task) {
        final CompletableFuture<ResultType> futureResult = new CompletableFuture<>();
        if (lifecycleState.get() == STATE_CLOSED) {
            futureResult.completeExceptionally(
                    new RejectedExecutionException("Structure is closed"));
            return futureResult;
        }
        final int previousSource = enterTask(sourceId);
        ResultType value = null;
        Throwable failure = null;
        try {
            value = task.get();
        } catch (final Throwable thrown) {
            failure = thrown;
        } finally {
            try {
                leaveTask(previousSource);
            } catch (final Throwable ignored) {
            }
        }
        complete(futureResult, value, failure);
        return futureResult;
    }

    /**
     * @param <ResultType>  result of the task
     * @param futureResult  future to complete
     * @param value         what the task returned, if it returned
     * @param failure       what it threw, or null
     */
    private static <ResultType> void complete(final CompletableFuture<ResultType> futureResult,
                                              final ResultType value,
                                              final Throwable failure) {
        if (failure == null) {
            futureResult.complete(value);
        } else {
            futureResult.completeExceptionally(failure);
        }
    }

    private int enterTask(final int sourceId) {
        final int previousSource = currentSourceId;
        if (sourceId != INHERIT_SOURCE) {
            currentSourceId = sourceId;
        }
        taskDepth++;
        return previousSource;
    }

    private void leaveTask(final int previousSource) {
        currentSourceId = previousSource;
        if (--taskDepth == 0) {
            runPostCommitDrains();
        }
    }

    /**
     * Hands every view what it owes, without waiting for a delivery interval to come round.
     *
     * @return future completed when every pending view has been drained
     */
    public CompletableFuture<Void> flushAll() {
        final Supplier<Void> task = new Supplier<Void>() {
            @Override
            public Void get() {
                drainPendingViews();
                return null;
            }
        };
        return submit(task);
    }

    public CompletableFuture<Void> run(final Runnable task) {
        return submitScoped(INHERIT_SOURCE, asSupplier(task));
    }

    /**
     * Runs a task attributing every property it writes to {@code sourceId}.
     *
     * @param sourceId the caller's own number for the writer, or {@link #NO_SOURCE}
     * @param task     the task to run on the worker
     * @return future completed when the task has run
     */
    public CompletableFuture<Void> run(final int sourceId, final Runnable task) {
        return submit(sourceId, asSupplier(task));
    }

    private static Supplier<Void> asSupplier(final Runnable task) {
        return new Supplier<Void>() {
            @Override
            public Void get() {
                task.run();
                return null;
            }
        };
    }

    public Node createNode(final String nodeName, final String nodeType) {
        return createNode(nodeName, nodeType, null);
    }

    /**
     * @param nodeName   name of the node
     * @param nodeType   type of the node
     * @param externalId the id the thing carries in the world that owns it, unique within this
     *                   structure and the node's for life; null if it models nothing outside
     * @return the node
     */
    public Node createNode(final String nodeName,
                           final String nodeType,
                           final CharSequence externalId) {
        requireTask();
        final String owned = ownedExternalId(externalId);
        requireFreeExternalId(owned);
        final long id = globalIdCounter.getAndIncrement();
        final Node newNode = new Node(id, nodeName, nodeType, owned);
        registerNewObject(newNode);
        return newNode;
    }

    public Link createLink(final String linkName,
                           final String linkType,
                           final long fromOutputId,
                           final long toInputId) {
        requireTask();
        final Output fromOutput =
                (Output) requireObject(fromOutputId, ObjectKind.OUTPUT);
        final Input toInput =
                (Input) requireObject(toInputId, ObjectKind.INPUT);
        final long id = globalIdCounter.getAndIncrement();
        final Link newLink = new Link(id, linkName, linkType, fromOutput, toInput);
        indexLinkAdd(newLink);
        registerNewObject(newLink);
        restateLinks(newLink);
        return newLink;
    }

    /**
     * @param nodeId    node the input belongs to
     * @param inputName its name; one input of a node with an external id has it
     * @param inputType type of the input
     * @return the input, known by {@link Port#appendExternalId} if its node has an external id
     */
    public Input addInput(final long nodeId,
                          final String inputName,
                          final String inputType) {
        requireTask();
        final Node owningNode =
                (Node) requireObject(nodeId, ObjectKind.NODE);
        final String owned = portExternalIdOf(owningNode, ObjectKind.INPUT, inputName);
        requireFreeExternalId(owned);
        final long id = globalIdCounter.getAndIncrement();
        final Input newInput = new Input(id, inputName, inputType, owningNode, owned);
        owningNode.inputsMap().put(id, newInput);
        registerNewObject(newInput);
        return newInput;
    }

    /**
     * @param nodeId     node the output belongs to
     * @param outputName its name; one output of a node with an external id has it
     * @param outputType type of the output
     * @return the output, known by {@link Port#appendExternalId} if its node has an external id
     */
    public Output addOutput(final long nodeId,
                            final String outputName,
                            final String outputType) {
        requireTask();
        final Node owningNode =
                (Node) requireObject(nodeId, ObjectKind.NODE);
        final String owned = portExternalIdOf(owningNode, ObjectKind.OUTPUT, outputName);
        requireFreeExternalId(owned);
        final long id = globalIdCounter.getAndIncrement();
        final Output newOutput =
                new Output(id, outputName, outputType, owningNode, owned);
        owningNode.outputsMap().put(id, newOutput);
        registerNewObject(newOutput);
        return newOutput;
    }

    private static String portExternalIdOf(final Node node, final ObjectKind side, final String name) {
        if (node.externalId() == null) {
            return null;
        }
        if (name == null || isBlank(name)) {
            throw new IllegalArgumentException("A port of a node with an external id is named");
        }
        return Port.appendExternalId(new StringBuilder(), node.externalId(), side, name).toString();
    }

    /**
     * Makes a view: a live set of objects, a selector's, with the ports of the links among them,
     * delivered to its subscribers as they change. A view watches the structure and is not
     * part of it.
     *
     * @param viewName               what to call it
     * @param selectorText           what it holds
     * @param propertyKeysOfInterest which properties it carries, or null or empty for all
     * @param deliveryPolicy         how often it delivers
     * @return the view
     */
    public CompletableFuture<View> createView(final String viewName,
                                              final String selectorText,
                                              final Set<String> propertyKeysOfInterest,
                                              final DeliveryPolicy deliveryPolicy) {
        return submit(new Supplier<View>() {
            @Override
            public View get() {
                if (deliveryPolicy == null) {
                    throw new IllegalArgumentException("deliveryPolicy is required");
                }
                final long id = globalIdCounter.getAndIncrement();
                final View newView = new View(id, viewName, selectorText,
                        propertyKeysOfInterest, deliveryPolicy, Structure.this);
                addView(newView);
                versionCounter.incrementAndGet();
                rebuildViewMembership(newView);
                return newView;
            }
        });
    }

    /**
     * @return the ids of the views there are
     */
    public CompletableFuture<long[]> viewIds() {
        return submit(() -> {
            final View[] held = views;
            final long[] ids = new long[held.length];
            for (int i = 0; i < held.length; i++) {
                ids[i] = held[i].id();
            }
            return ids;
        });
    }

    /**
     * Removes a view; its subscribers are told it has gone.
     *
     * @param viewId the view
     * @return completes when it is gone
     */
    public CompletableFuture<Void> removeView(final long viewId) {
        return run(new Runnable() {
            @Override
            public void run() {
                final View view = viewsById.get(viewId);
                if (view == null) {
                    return;
                }
                view.terminateSubscribers(new IllegalStateException("View removed from structure"));
                removeView(view);
                versionCounter.incrementAndGet();
            }
        });
    }

    /**
     * Sets a whole-number property. Runs inside a task.
     *
     * @param objectId object to write to
     * @param keyId    property key id, from {@link #propertyKeys()}
     * @param value    the value
     */
    public void setLong(final long objectId, final int keyId, final long value) {
        final StructureObject target = requireWritableProperty(objectId, keyId);
        final boolean folded = foldedBefore(target, keyId);
        if (target.properties().setLong(keyId, value, currentSourceId)) {
            commitPropertyChange(target, keyId, folded);
        }
    }

    /**
     * Sets a fractional property. Runs inside a task.
     *
     * @param objectId object to write to
     * @param keyId    property key id, from {@link #propertyKeys()}
     * @param value    the value
     */
    public void setDouble(final long objectId, final int keyId, final double value) {
        final StructureObject target = requireWritableProperty(objectId, keyId);
        final boolean folded = foldedBefore(target, keyId);
        if (target.properties().setDouble(keyId, value, currentSourceId)) {
            commitPropertyChange(target, keyId, folded);
        }
    }

    /**
     * Sets a flag property. Runs inside a task.
     *
     * @param objectId object to write to
     * @param keyId    property key id, from {@link #propertyKeys()}
     * @param value    the value
     */
    public void setBoolean(final long objectId, final int keyId, final boolean value) {
        final StructureObject target = requireWritableProperty(objectId, keyId);
        final boolean folded = foldedBefore(target, keyId);
        if (target.properties().setBoolean(keyId, value, currentSourceId)) {
            commitPropertyChange(target, keyId, folded);
        }
    }

    /**
     * Sets a text property. The structure keeps the string it is given.
     * Runs inside a task.
     *
     * @param objectId object to write to
     * @param keyId    property key id, from {@link #propertyKeys()}
     * @param value    the value
     */
    public void setText(final long objectId, final int keyId, final String value) {
        final StructureObject target = requireWritableProperty(objectId, keyId);
        final boolean folded = foldedBefore(target, keyId);
        final boolean changed = value == null
                ? target.properties().remove(keyId)
                : target.properties().setText(keyId, value, currentSourceId);
        if (changed) {
            commitPropertyChange(target, keyId, folded);
        }
    }

    /**
     * Drops a property. Runs inside a task.
     *
     * @param objectId object to write to
     * @param keyId    property key id, from {@link #propertyKeys()}
     */
    public void removeProperty(final long objectId, final int keyId) {
        final StructureObject target = requireWritableProperty(objectId, keyId);
        final boolean folded = foldedBefore(target, keyId);
        if (target.properties().remove(keyId)) {
            commitPropertyChange(target, keyId, folded);
        }
    }

    /**
     * Re-states every property of an object to the views holding it, without changing any.
     * Runs inside a task.
     *
     * @param objectId object to re-state
     */
    public void touch(final long objectId) {
        requireTask();
        final StructureObject target = requireObject(objectId);
        versionCounter.incrementAndGet();
        final View[] snapshot = views;
        for (int i = 0; i < snapshot.length; i++) {
            final View view = snapshot[i];
            if (view.matchedObjectIds().contains(objectId)) {
                view.markEveryPropertyDirty(target);
            }
        }
        recheck(target);
    }

    /**
     * Says what one client wants, replacing whatever it wanted before. Demand is the highest
     * level any client asks for, so a thing one window has collapsed stays whole while
     * another window is showing it.
     *
     * @param client   the caller's own name for the client
     * @param interest what it wants
     * @return done when the sources have been told
     */
    public CompletableFuture<Void> setInterest(final CharSequence client,
                                               final Interest interest) {
        if (isBlank(client)) {
            throw new IllegalArgumentException("client must not be null or blank");
        }
        if (interest == null) {
            throw new IllegalArgumentException("interest is required");
        }
        final String name = client.toString();
        final Interest held = interest.copy();
        return run(new Runnable() {
            @Override
            public void run() {
                final Interest was = interests.get(name);
                interests.put(name, held);
                if (was != null && was.base() == held.base()) {
                    applyDemandAt(was);         // what neither names is wanted at the same base
                    applyDemandAt(held);
                } else {
                    applyDemand();
                }
            }
        });
    }

    /**
     * Forgets what a client wanted - a window closing. With the last interest gone, nobody
     * has spoken again and everything is wanted whole.
     *
     * @param client the caller's own name for the client
     * @return done when the sources have been told
     */
    public CompletableFuture<Void> clearInterest(final CharSequence client) {
        if (isBlank(client)) {
            throw new IllegalArgumentException("client must not be null or blank");
        }
        final String name = client.toString();
        return run(new Runnable() {
            @Override
            public void run() {
                if (interests.get(name) != null) {   // a client that never spoke changes nothing
                    interests.remove(name);
                    applyDemand();
                }
            }
        });
    }

    /**
     * Walks the ids as they were: telling a source can drop a group, and a removal moves the last
     * object into the hole. What is made meanwhile is told as it is registered.
     */
    private void applyDemand() {
        final long[] ids = new long[objectsById.size()];
        for (int i = 0; i < ids.length; i++) {
            ids[i] = objectsById.keyAt(i);
        }
        for (int i = 0; i < ids.length; i++) {
            final StructureObject object = objectsById.get(ids[i]);
            if (object != null) {
                applyDemandTo(object);
            }
        }
    }

    private void applyDemandAt(final Interest interest) {
        for (int i = 0; i < interest.overrideCount(); i++) {
            final StructureObject object = objectsById.get(interest.overrideIdAt(i));
            if (object != null) {
                applyDemandTo(object);
            }
        }
    }

    private void applyDemandTo(final StructureObject object) {
        final int wanted = demandedLevel(object.id());
        if (object.requireLevel(wanted)) {
            announceDemand(object, wanted);
        }
    }

    private int demandedLevel(final long objectId) {
        if (interests.size() == 0) {
            return DetailLevel.FINE.ordinal();
        }
        int highest = DetailLevel.OFF.ordinal();
        for (int i = 0; i < interests.size(); i++) {
            final int wanted = interests.valueAt(i).ordinalOf(objectId);
            if (wanted > highest) {
                highest = wanted;
            }
        }
        return highest;
    }

    private void announceDemand(final StructureObject object, final int level) {
        final DemandListener[] snapshot = demandListeners;
        for (int i = 0; i < snapshot.length; i++) {
            snapshot[i].demandChanged(object, DetailLevel.of(level));
        }
    }

    int requiredLevelOrdinal(final long objectId) {
        final StructureObject target = objectsById.get(objectId);
        return target == null ? DetailLevel.FINE.ordinal() : target.requiredLevelOrdinal();
    }

    synchronized void watchDemand(final DemandListener listener) {
        final DemandListener[] current = demandListeners;
        final DemandListener[] grown = new DemandListener[current.length + 1];
        System.arraycopy(current, 0, grown, 0, current.length);
        grown[current.length] = listener;
        demandListeners = grown;
    }

    synchronized void unwatchDemand(final DemandListener listener) {
        final DemandListener[] current = demandListeners;
        int at = -1;
        for (int i = 0; i < current.length; i++) {
            if (current[i] == listener) {
                at = i;
                break;
            }
        }
        if (at < 0) {
            return;
        }
        final DemandListener[] shrunk = new DemandListener[current.length - 1];
        System.arraycopy(current, 0, shrunk, 0, at);
        System.arraycopy(current, at + 1, shrunk, at, shrunk.length - at);
        demandListeners = shrunk;
    }

    /**
     * @param externalId the id as the caller holds it, possibly a buffer it reuses
     * @return a copy of it the structure can keep, or null for none
     */
    private static String ownedExternalId(final CharSequence externalId) {
        if (externalId == null) {
            return null;
        }
        if (isBlank(externalId)) {
            throw new IllegalArgumentException("externalId must not be blank");
        }
        return externalId.toString();
    }

    private void requireFreeExternalId(final String externalId) {
        if (externalId == null) {
            return;
        }
        final StructureObject holder = objectsByExternalId.get(externalId);
        if (holder != null) {
            throw new IllegalArgumentException(
                    "External id '" + externalId + "' is already held by object " + holder.id());
        }
        final int colon = externalId.indexOf(':');
        if (colon > 0 && groupingsByName.get(externalId.substring(0, colon)) != null) {
            throw new IllegalArgumentException(
                    "External id '" + externalId + "' is under the name of a grouping");
        }
    }

    private void indexExternalId(final StructureObject object) {
        if (object.externalId() != null) {
            objectsByExternalId.put(object.externalId(), object);
        }
    }

    /**
     * Resolves the id a thing carries in its own world into the object modelling it; the id is
     * read as characters, so a reused StringBuilder searches without allocating. Runs on the
     * structure's own thread.
     *
     * @param externalId the id to look up
     * @return the object holding the id, or null if the structure has never been told it
     */
    public StructureObject findByExternalId(final CharSequence externalId) {
        requireWorkerThread();
        return objectsByExternalId.get(externalId);
    }

    private static boolean isBlank(final CharSequence text) {
        if (text == null) {
            return true;
        }
        for (int i = 0; i < text.length(); i++) {
            if (!Character.isWhitespace(text.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    private void forgetExternalId(final StructureObject target) {
        final String key = target.externalId();
        if (key != null) {
            objectsByExternalId.remove(key);
        }
    }

    /**
     * Says the thing a port stands for is reachable at an address - a queue's name, a
     * service's URL. The structure draws a link to every port of the opposite kind that
     * requires the same address, whichever of the two was declared first, and takes it away
     * when either declaration goes. The address is one opaque text: it is compared whole and
     * never read into, so whoever writes it decides what it is made of.
     * Runs inside a task.
     *
     * @param portId  the input or output the thing is reachable at
     * @param address the address, spelled as the world that owns it spells it
     */
    public void provide(final long portId, final CharSequence address) {
        declareAddress(portId, address, Domains.NO_DOMAIN, Role.PROVIDE);
    }

    /**
     * Provides an address in one copy of the world: blue and green publish under the same
     * names, and the domain is what tells their addresses apart.
     *
     * @param portId   the input or output the thing is reachable at
     * @param address  the address
     * @param domainId the copy of the world it is reachable in
     */
    public void provide(final long portId, final CharSequence address, final int domainId) {
        declareAddress(portId, address, domainId, Role.PROVIDE);
    }

    /**
     * Says a port is looking for whatever is reachable at an address. Nothing has to be
     * there yet: the link appears when something provides it.
     * Runs inside a task.
     *
     * @param portId  the input or output looking for it
     * @param address the address it is looking for
     */
    public void require(final long portId, final CharSequence address) {
        declareAddress(portId, address, Domains.NO_DOMAIN, Role.REQUIRE);
    }

    /**
     * Requires an address from one copy of the world. Which copy answers is the provider's
     * to say, so a requirer reaches its own domain, and another only where a route allows.
     *
     * @param portId   the input or output looking for it
     * @param address  the address it is looking for
     * @param domainId the copy of the world it is looking from
     */
    public void require(final long portId, final CharSequence address, final int domainId) {
        declareAddress(portId, address, domainId, Role.REQUIRE);
    }

    /**
     * Provides several addresses at one port, each matched on its own, in place of whatever it
     * declared: a port that answers for many things meets each of their requirers. Declaring
     * none withdraws.
     * Runs inside a task.
     *
     * @param portId    the input or output they are reachable at
     * @param addresses the addresses, in any order
     * @param domainId  the copy of the world they are reachable in
     */
    public void provide(final long portId, final Collection<? extends CharSequence> addresses, final int domainId) {
        declareAddresses(portId, addresses, domainId, Role.PROVIDE);
    }

    /**
     * Requires several addresses at one port, each matched on its own, in place of whatever it
     * declared: an input that reads many streams of a store meets the output of each, and a
     * changed set redraws only the links of the addresses that came or went. Requiring none
     * withdraws.
     * Runs inside a task.
     *
     * @param portId    the input or output looking for them
     * @param addresses the addresses, in any order
     * @param domainId  the copy of the world it is looking from
     */
    public void require(final long portId, final Collection<? extends CharSequence> addresses, final int domainId) {
        declareAddresses(portId, addresses, domainId, Role.REQUIRE);
    }

    /**
     * Withdraws whatever a port declared, taking the links drawn from it with it.
     * Runs inside a task.
     *
     * @param portId the input or output
     */
    public void clearAddress(final long portId) {
        requireTask();
        withdrawAddress(requirePort(portId));
    }

    /**
     * @param portId    port declaring
     * @param address   what it provides or requires
     * @param domainId  which copy of the world
     * @param role      whether the address is offered or looked for
     */
    private void declareAddress(final long portId,
                                final CharSequence address,
                                final int domainId,
                                final Role role) {
        requireTask();
        if (isBlank(address)) {
            throw new IllegalArgumentException("address must not be null or blank");
        }
        domains.requireKnown(domainId);
        final Port port = requirePort(portId);
        if (port.declaration() != null && port.declaration().sameAs(address, role, domainId)) {
            return;                     // an observation repeating itself costs nothing
        }
        declare(port, new String[] {address.toString()}, domainId, role);
    }

    private void declareAddresses(final long portId,
                                  final Collection<? extends CharSequence> addresses,
                                  final int domainId,
                                  final Role role) {
        requireTask();
        domains.requireKnown(domainId);
        final Port port = requirePort(portId);
        if (port.declaration() != null && port.declaration().sameAs(addresses, role, domainId)) {
            return;
        }
        final String[] made = new String[addresses.size()];
        int count = 0;
        for (final CharSequence address : addresses) {
            if (isBlank(address)) {
                throw new IllegalArgumentException("address must not be null or blank");
            }
            made[count++] = address.toString();
        }
        Arrays.sort(made);
        count = 0;
        for (int i = 0; i < made.length; i++) {
            if (count == 0 || !made[i].equals(made[count - 1])) {
                made[count++] = made[i];
            }
        }
        if (count == 0) {
            withdrawAddress(port);
        } else {
            declare(port, count == made.length ? made : Arrays.copyOf(made, count), domainId, role);
        }
    }

    /**
     * Keeps the links of the addresses the port still declares: with the same role in the same
     * domain only those of the addresses that went are taken away and of those that came drawn.
     *
     * @param port      the port
     * @param addresses sorted, each once
     * @param domainId  the copy of the world
     * @param role      what it does with them
     */
    private void declare(final Port port, final String[] addresses, final int domainId, final Role role) {
        final Port.Declaration held = port.declaration();
        if (held != null && held.role == role && held.domainId == domainId) {
            if (Arrays.equals(held.addresses, addresses)) {
                return;
            }
            final Port.Declaration made = new Port.Declaration(addresses, role, domainId, domains.nameOf(domainId));
            for (final String address : held.addresses) {
                if (!made.declares(address)) {
                    removeDerivedLinksAt(port, address);
                    unindexAddress(port, address);
                }
            }
            port.declare(made);
            versionCounter.incrementAndGet();
            markDeclarationDirty(port);
            recheck(port);
            for (final String address : addresses) {
                if (!held.declares(address)) {
                    linkToPeersOf(port, address, indexAddress(port, address));
                }
            }
            return;
        }
        withdrawAddress(port);
        port.declare(new Port.Declaration(addresses, role, domainId, domains.nameOf(domainId)));
        if (role == Role.REQUIRE) {
            LongSet requiring = requiringPortIdsByDomain.get(domainId);
            if (requiring == null) {
                requiring = new LongSet();
                requiringPortIdsByDomain.put(domainId, requiring);
            }
            requiring.add(port.id());
        }
        for (final String address : addresses) {
            indexAddress(port, address);
        }
        versionCounter.incrementAndGet();
        markDeclarationDirty(port);
        recheck(port);
        for (final String address : addresses) {
            linkToPeersOf(port, address, portIdsByAddress.get(address));
        }
    }

    /**
     * @param port    the port
     * @param address one it declares
     * @return the ports declaring the address, the port among them
     */
    private LongSet indexAddress(final Port port, final String address) {
        LongSet peers = portIdsByAddress.get(address);
        if (peers == null) {
            peers = new LongSet();
            portIdsByAddress.put(address, peers);
        }
        peers.add(port.id());
        return peers;
    }

    private void unindexAddress(final Port port, final String address) {
        final LongSet peers = portIdsByAddress.get(address);
        if (peers != null) {
            peers.remove(port.id());
            if (peers.size() == 0) {
                portIdsByAddress.remove(address);
            }
        }
    }

    private void withdrawAddress(final Port port) {
        if (port.declaration() == null) {
            return;
        }
        removeDerivedLinksAt(port, null);
        forgetAddress(port);
        port.undeclare();
        versionCounter.incrementAndGet();
        markDeclarationDirty(port);
        recheck(port);
    }

    private void markDeclarationDirty(final Port port) {
        markDirtyInAllViews(port, PropertyKeys.ADDRESS);
        markDirtyInAllViews(port, PropertyKeys.ROLE);
        markDirtyInAllViews(port, PropertyKeys.DOMAIN);
    }

    private void forgetAddress(final StructureObject target) {
        if (!(target instanceof Port)) {
            return;
        }
        final Port port = (Port) target;
        if (port.declaration() == null) {
            return;
        }
        for (final String address : port.declaration().addresses) {
            unindexAddress(port, address);
        }
        if (port.role() == Role.REQUIRE) {
            final LongSet requiring = requiringPortIdsByDomain.get(port.domain());
            if (requiring != null) {
                requiring.remove(port.id());
                if (requiring.size() == 0) {
                    requiringPortIdsByDomain.remove(port.domain());
                }
            }
        }
    }

    private void linkToPeersOf(final Port declared, final String address, final LongSet peers) {
        for (int i = 0; i < peers.size(); i++) {
            final long peerId = peers.valueAt(i);
            if (peerId == declared.id()) {
                continue;
            }
            final StructureObject peer = objectsById.get(peerId);
            if (peer instanceof Port) {
                linkIfMatched(declared, (Port) peer, address);
            }
        }
    }

    /**
     * Draws the link two declarations imply: one port offers the address, the other looks
     * for it, and they are opposite ends, so the link runs from the output to the input.
     * It is named by the address it came from and typed by the port that offered it.
     *
     * @param left    one declaring port
     * @param right   the other
     * @param address what both declare
     */
    private void linkIfMatched(final Port left, final Port right, final String address) {
        if (left.role() == right.role() || left.kind() == right.kind()) {
            return;
        }
        final Port provider = left.role() == Role.PROVIDE ? left : right;
        final Port requirer = left.role() == Role.PROVIDE ? right : left;
        if (!isRouteAllowed(requirer.domain(), provider.domain())) {
            return;
        }
        final boolean providerIsOutput = provider.kind() == ObjectKind.OUTPUT;
        final Output fromOutput = (Output) (providerIsOutput ? provider : requirer);
        final Input toInput = (Input) (providerIsOutput ? requirer : provider);
        final long id = globalIdCounter.getAndIncrement();
        final Link derivedLink = new Link(
                id, address, provider.type(), fromOutput, toInput, true, null);
        indexLinkAdd(derivedLink);
        registerNewObject(derivedLink);
        restateLinks(derivedLink);
    }

    /**
     * Takes away the links a port's declaration drew, leaving the ones someone made by
     * hand. A removal moves the last link of the index into the hole, so walking backwards
     * visits each one exactly once.
     *
     * @param port    the port whose declaration is going
     * @param address the one of its addresses that is going, or null for all
     */
    private void removeDerivedLinksAt(final Port port, final String address) {
        final LongSet linkIds = linksAt(port);
        if (linkIds == null) {
            return;
        }
        for (int i = linkIds.size() - 1; i >= 0; i--) {
            final StructureObject candidate = objectsById.get(linkIds.valueAt(i));
            if (candidate instanceof Link && ((Link) candidate).isDerived()
                    && (address == null || address.equals(candidate.name()))) {
                cascadeRemoveSingle(candidate);
            }
        }
    }

    /**
     * Lets requirers in one domain reach providers in another - the route that hands blue's
     * traffic to green. A domain always reaches itself and what belongs to no domain, so only
     * crossings between named ones are declared, and declaring one draws the links it newly permits.
     *
     * @param requiringDomain the domain looking for an address
     * @param providingDomain the domain that may answer it
     */
    public void allowRoute(final int requiringDomain,
                           final int providingDomain) {
        requireTask();
        domains.requireKnown(requiringDomain);
        domains.requireKnown(providingDomain);
        if (requiringDomain == providingDomain || providingDomain == Domains.NO_DOMAIN
                || !allowedRoutes.add(routeKey(requiringDomain, providingDomain))) {
            return;
        }
        linkAcrossRoute(requiringDomain, providingDomain);
    }

    /**
     * Closes a route, taking with it the links that only it allowed.
     *
     * @param requiringDomain the domain that was looking
     * @param providingDomain the domain that was answering
     */
    public void denyRoute(final int requiringDomain,
                          final int providingDomain) {
        requireTask();
        if (!allowedRoutes.remove(routeKey(requiringDomain, providingDomain))) {
            return;
        }
        unlinkAcrossRoute(requiringDomain, providingDomain);
        versionCounter.incrementAndGet();
    }

    /**
     * A requirer reaches its own copy of the world, and whatever belongs to no copy at all -
     * the shared queue both colours read from. Reaching another copy takes a route, because
     * which of them answers is not the requirer's to decide.
     *
     * @param requiringDomain the domain looking for an address
     * @param providingDomain the domain offering it
     * @return whether a link may span the two
     */
    private boolean isRouteAllowed(final int requiringDomain, final int providingDomain) {
        return requiringDomain == providingDomain
                || providingDomain == Domains.NO_DOMAIN
                || allowedRoutes.contains(routeKey(requiringDomain, providingDomain));
    }

    private static long routeKey(final int requiringDomain, final int providingDomain) {
        return ((long) requiringDomain << 32) | (providingDomain & 0xFFFFFFFFL);
    }

    /**
     * Visits only what the route can join: the ports looking in one domain, and the ports
     * declaring the same address as each of them.
     *
     * @param requiringDomain the domain of the side that has to be looking
     * @param providingDomain the domain of the side that has to be offering
     */
    private void linkAcrossRoute(final int requiringDomain, final int providingDomain) {
        final LongSet requiring = requiringPortIdsByDomain.get(requiringDomain);
        if (requiring == null) {
            return;
        }
        for (int i = 0; i < requiring.size(); i++) {
            final Port requirer = (Port) objectsById.get(requiring.valueAt(i));
            for (final String address : requirer.declaration().addresses) {
                final LongSet peers = portIdsByAddress.get(address);
                for (int k = 0; k < peers.size(); k++) {
                    final StructureObject peer = objectsById.get(peers.valueAt(k));
                    if (peer instanceof Port && ((Port) peer).domain() == providingDomain) {
                        linkIfMatched(requirer, (Port) peer, address);
                    }
                }
            }
        }
    }

    /**
     * Visits only the links at the ports looking in one domain. A removal moves the last
     * link of the index into the hole, so walking backwards visits each one exactly once.
     *
     * @param requiringDomain the domain that was looking
     * @param providingDomain the domain that was answering
     */
    private void unlinkAcrossRoute(final int requiringDomain, final int providingDomain) {
        final LongSet requiring = requiringPortIdsByDomain.get(requiringDomain);
        if (requiring == null) {
            return;
        }
        for (int i = 0; i < requiring.size(); i++) {
            final Port requirer = (Port) objectsById.get(requiring.valueAt(i));
            final LongSet linkIds = linksAt(requirer);
            if (linkIds == null) {
                continue;
            }
            for (int k = linkIds.size() - 1; k >= 0; k--) {
                final Link link = (Link) objectsById.get(linkIds.valueAt(k));
                if (!link.isDerived()) {
                    continue;
                }
                final Port provider = link.fromOutput() == requirer
                        ? link.toInput() : link.fromOutput();
                if (provider.domain() == providingDomain) {
                    cascadeRemoveSingle(link);
                }
            }
        }
    }

    /**
     * A link the structure drew is the declarations' to keep: it is not removed or rewired
     * on its own, it goes when a declaration does.
     *
     * @param link the link a caller is about to change
     */
    private static void requireNotDerived(final Link link) {
        if (link.isDerived()) {
            throw new IllegalArgumentException(
                    "Link " + link.id() + " is drawn from an address; change the declaration");
        }
    }

    private Port requirePort(final long portId) {
        final StructureObject candidate = requireObject(portId);
        if (!(candidate instanceof Port)) {
            throw new IllegalArgumentException(
                    "Object '" + portId + "' is " + candidate.kind() + ", expected a port");
        }
        return (Port) candidate;
    }

    private StructureObject requireWritableProperty(final long objectId, final int keyId) {
        requireTask();
        if (PropertyKeys.isReserved(keyId)) {
            throw new IllegalArgumentException(
                    "Cannot set reserved property: " + propertyKeys.nameOf(keyId));
        }
        final StructureObject target = requireObject(objectId);
        if (target.derives(keyId)) {
            throw new IllegalArgumentException("Property '" + propertyKeys.nameOf(keyId)
                    + "' of '" + objectId + "' is derived");
        }
        final Grouping grouping = groupingOfNode.get(objectId);
        if (grouping != null && grouping.keyedBy(keyId)) {
            throw new IllegalArgumentException("Property '" + propertyKeys.nameOf(keyId)
                    + "' of '" + objectId + "' is the key of a group of '" + grouping.name() + "'");
        }
        return target;
    }

    private void commitPropertyChange(final StructureObject target, final int keyId) {
        versionCounter.incrementAndGet();
        markDirtyInAllViews(target, keyId);
        recheck(target, keyId);
        if (keyId < ownFoldedKeys.length && ownFoldedKeys[keyId] > 0) {
            foldOwnAfter(target, keyId);
        }
    }

    private void commitPropertyChange(final StructureObject target,
                                      final int keyId,
                                      final boolean folded) {
        commitPropertyChange(target, keyId);
        if (folded) {
            foldAfter(target, keyId);
        }
    }

    /**
     * Makes a property the fold of what {@code over} names: properties of the object itself,
     * a property of its children, or both. Over children the object also gets how many
     * summands are known and how many there are, as {@code <key>.known} and
     * {@code <key>.total}. Declaring the same thing again is nothing; declaring something else
     * for the same key, deriving a key a source already writes there or a group's key, or
     * folding a derived key of the object itself, is an error: a key is either supplied or
     * derived, never both.
     * Runs inside a task.
     *
     * @param objectId the object
     * @param keyId    what to write on it
     * @param fold     how the summands come to one value
     * @param over     what the summands are
     */
    public void derive(final long objectId, final int keyId, final Fold fold, final Over over) {
        requireTask();
        requireFoldable(keyId, fold, over);
        final StructureObject target = requireObject(objectId);
        if (!requireDerivable(target, keyId, fold, over)) {
            return;
        }
        final String name = propertyKeys.nameOf(keyId);
        final Derivation derivation = new Derivation(keyId,
                propertyKeys.idOf(name + ".known"), propertyKeys.idOf(name + ".total"), fold, over,
                over.overMembers() ? MemberPath.parse(over.memberPath, propertyKeys) : null);
        if (over.overChildren()) {
            foldedKeys = counted(foldedKeys, over.childKeyId, 1);
        }
        for (int i = 0; i < over.ownKeyIds.length; i++) {
            ownFoldedKeys = counted(ownFoldedKeys, over.ownKeyIds[i], 1);
        }
        final boolean folded = foldedBefore(target, keyId);
        target.addDerivation(derivation);
        retally(target, derivation);
        writeDerived(target, derivation);   // what was written there by hand is not the fold's
        derivation.published.set(derivation.tally);
        if (folded) {
            foldDerivingChild(target, keyId);
        }
    }

    /**
     * A child has started deriving a key its parents fold: what it brought as a value, in
     * {@link #leafWas}, is replaced by what it brings now - its summands, or its derived value.
     *
     * @param child the child
     * @param keyId the key it derives
     */
    private void foldDerivingChild(final StructureObject child, final int keyId) {
        final LongSet parents = parentsByChild.get(child.id());
        for (int i = 0; i < parents.size(); i++) {
            final StructureObject parent = objectsById.get(parents.valueAt(i));
            final Derivation[] derivations = parent.derivations();
            for (int k = 0; derivations != null && k < derivations.length; k++) {
                final Derivation derivation = derivations[k];
                if (derivation.over.childKeyId != keyId) {
                    continue;
                }
                final Derivation.Tally was = derivation.takes(child) ? leafWas : nothing;
                final Derivation.Tally now = summandOf(derivation, child);
                if (!derivation.tally.replace(was, now != null ? now : nothing)) {
                    retally(parent, derivation);
                }
                publish(parent, derivation);
            }
        }
    }

    /**
     * @param keyId what to write
     * @param fold  how the summands come to one value
     * @param over  what the summands are
     */
    private void requireFoldable(final int keyId, final Fold fold, final Over over) {
        if (fold == null || over == null) {
            throw new IllegalArgumentException("fold and over are required");
        }
        if (PropertyKeys.isReserved(keyId)) {
            throw new IllegalArgumentException(
                    "Cannot derive reserved property: " + propertyKeys.nameOf(keyId));
        }
        if (over.overMembers()) {
            MemberPath.parse(over.memberPath, propertyKeys);
        }
    }

    /**
     * @param target the object
     * @param keyId  what to write on it
     * @param fold   how the summands come to one value
     * @param over   what the summands are
     * @return false if it is derived so already; throws if it cannot be
     */
    private boolean requireDerivable(final StructureObject target,
                                     final int keyId,
                                     final Fold fold,
                                     final Over over) {
        final long objectId = target.id();
        if (over.overChildren() && !(target instanceof Node)) {
            throw new IllegalArgumentException("Only a node folds its children: " + objectId);
        }
        if (over.overMembers() && groupingOfNode.get(objectId) == null) {
            throw new IllegalArgumentException("Only a group a grouping made folds its members: " + objectId);
        }
        final Derivation held = target.derivation(keyId);
        if (held != null) {
            if (held.sameAs(fold, over)) {
                return false;
            }
            throw new IllegalArgumentException("Property '" + propertyKeys.nameOf(keyId)
                    + "' of '" + objectId + "' is already derived otherwise");
        }
        for (int i = 0; i < over.ownKeyIds.length; i++) {
            final int own = over.ownKeyIds[i];
            if (own == keyId || target.derives(own)) {
                throw new IllegalArgumentException("Property '" + propertyKeys.nameOf(own)
                        + "' of '" + objectId + "' is derived and cannot be folded on it");
            }
        }
        final Derivation[] others = target.derivations();
        for (int i = 0; others != null && i < others.length; i++) {
            if (others[i].over.readsOwn(keyId)) {
                throw new IllegalArgumentException("Property '" + propertyKeys.nameOf(keyId)
                        + "' of '" + objectId + "' is folded on it and cannot be derived");
            }
        }
        final Grouping grouping = groupingOfNode.get(objectId);
        if (grouping != null) {
            requireOutsideKey(grouping, keyId, over);
        }
        final String name = propertyKeys.nameOf(keyId);
        requireUnsupplied(target, keyId);
        if (over.counts()) {
            requireUnsupplied(target, propertyKeys.idOf(name + ".known"));
            requireUnsupplied(target, propertyKeys.idOf(name + ".total"));
        }
        return true;
    }

    /**
     * @param grouping a grouping
     * @param keyId    what its groups would derive
     * @param over     what the summands are
     */
    private void requireOutsideKey(final Grouping grouping, final int keyId, final Over over) {
        final String name = propertyKeys.nameOf(keyId);
        if (grouping.keyedBy(keyId) || over.counts()
                && (grouping.keyedBy(propertyKeys.idOf(name + ".known"))
                || grouping.keyedBy(propertyKeys.idOf(name + ".total")))) {
            throw new IllegalArgumentException("Property '" + name + "' of the groups of '"
                    + grouping.name() + "' would write their key");
        }
    }

    private static int[] counted(final int[] counts, final int keyId, final int delta) {
        final int[] held = keyId < counts.length ? counts : Arrays.copyOf(counts, keyId + 1);
        held[keyId] += delta;
        return held;
    }

    private void requireUnsupplied(final StructureObject target, final int keyId) {
        final int writer = target.sourceOf(keyId);
        if (target.valueTypeOf(keyId) != ValueType.ABSENT && writer != NO_SOURCE) {
            throw new IllegalArgumentException("Property '" + propertyKeys.nameOf(keyId)
                    + "' of '" + target.id() + "' is supplied by source " + writer);
        }
    }

    private void forgetDerivations(final StructureObject target) {
        final Derivation[] derivations = target.derivations();
        for (int i = 0; derivations != null && i < derivations.length; i++) {
            final Over over = derivations[i].over;
            if (over.overChildren()) {
                foldedKeys[over.childKeyId]--;
            }
            for (int k = 0; k < over.ownKeyIds.length; k++) {
                ownFoldedKeys[over.ownKeyIds[k]]--;
            }
        }
    }

    /**
     * @param target the object about to be written
     * @param keyId  the property
     * @return whether the write reaches a fold of its parents, having noted what the object
     *         was worth to it before the write
     */
    private boolean foldedBefore(final StructureObject target, final int keyId) {
        if (keyId >= foldedKeys.length || foldedKeys[keyId] == 0
                || parentsByChild.get(target.id()) == null) {
            return false;
        }
        leafWas.ofLeaf(target, keyId);
        return true;
    }

    private void foldAfter(final StructureObject target, final int keyId) {
        leafNow.ofLeaf(target, keyId);
        final LongSet parents = parentsByChild.get(target.id());
        for (int i = 0; i < parents.size(); i++) {
            final StructureObject parent = objectsById.get(parents.valueAt(i));
            final Derivation[] derivations = parent.derivations();
            for (int k = 0; derivations != null && k < derivations.length; k++) {
                final Derivation derivation = derivations[k];
                if (derivation.over.childKeyId == keyId && derivation.takes(target)) {
                    if (!derivation.tally.replace(leafWas, leafNow)) {
                        retally(parent, derivation);
                    }
                    publish(parent, derivation);
                }
            }
        }
    }

    /**
     * An object's own property was written: what it folds of its own properties is worked out
     * again.
     *
     * @param target the object
     * @param keyId  the property written
     */
    private void foldOwnAfter(final StructureObject target, final int keyId) {
        final Derivation[] derivations = target.derivations();
        for (int i = 0; derivations != null && i < derivations.length; i++) {
            if (derivations[i].over.readsOwn(keyId)) {
                retally(target, derivations[i]);
                publish(target, derivations[i]);
            }
        }
    }

    private void retally(final StructureObject target, final Derivation derivation) {
        derivation.tally.clear();
        if (derivation.over.overChildren()) {
            final LongObjectMap<Node> children = ((Node) target).childrenMap();
            for (int i = 0; i < children.size(); i++) {
                addSummand(derivation, children.valueAt(i));
            }
        }
        final int[] own = derivation.over.ownKeyIds;
        for (int i = 0; i < own.length; i++) {
            summand.ofLeaf(target, own[i]);
            derivation.tally.add(summand);
        }
        if (derivation.over.overMembers()) {
            final Grouping grouping = groupingOfNode.get(target.id());
            final LongSet members = grouping != null ? grouping.membersOf(target.id()) : null;
            for (int i = 0; members != null && i < members.size(); i++) {
                final long memberId = members.valueAt(i);
                memberSummand(derivation, objectsById.get(memberId));
                derivation.tally.add(summand);
                contributionOf(derivation, memberId).set(summand);
            }
        }
    }

    /**
     * @param derivation a fold over members
     * @param member     a member, or null if it has gone
     */
    private void memberSummand(final Derivation derivation, final StructureObject member) {
        final StructureObject holder = member != null ? reach(member, derivation.memberPath) : null;
        if (holder != null) {
            summand.ofLeaf(holder, derivation.memberPath.keyId);
        } else {
            summand.unknown();
        }
    }

    private static Derivation.Sum contributionOf(final Derivation derivation, final long memberId) {
        Derivation.Sum held = derivation.contributions.get(memberId);
        if (held == null) {
            held = new Derivation.Sum();
            derivation.contributions.put(memberId, held);
        }
        return held;
    }

    /**
     * A member has joined a group or changed: what it brings in replaces what it brought.
     *
     * @param group  the group
     * @param member the member
     */
    private void foldIn(final StructureObject group, final StructureObject member) {
        final Derivation[] derivations = group.derivations();
        for (int k = 0; derivations != null && k < derivations.length; k++) {
            final Derivation derivation = derivations[k];
            if (!derivation.over.overMembers()) {
                continue;
            }
            memberSummand(derivation, member);
            final Derivation.Sum was = derivation.contributions.get(member.id());
            if (was == null) {
                derivation.tally.add(summand);
                contributionOf(derivation, member.id()).set(summand);
            } else if (derivation.tally.replace(was, summand)) {
                was.set(summand);
            } else {
                retally(group, derivation);         // which sets what every member brings
            }
            publish(group, derivation);
        }
    }

    /**
     * A member has left a group: what it brought in is taken out.
     *
     * @param group    the group
     * @param memberId the member
     */
    private void foldOut(final StructureObject group, final long memberId) {
        final Derivation[] derivations = group.derivations();
        for (int k = 0; derivations != null && k < derivations.length; k++) {
            final Derivation derivation = derivations[k];
            if (!derivation.over.overMembers()) {
                continue;
            }
            final Derivation.Sum was = derivation.contributions.remove(memberId);
            if (was != null && !derivation.tally.replace(was, nothing)) {
                retally(group, derivation);
            }
            publish(group, derivation);
        }
    }

    private void addSummand(final Derivation derivation, final StructureObject child) {
        final Derivation.Tally brought = summandOf(derivation, child);
        if (brought != null) {
            derivation.tally.add(brought);
        }
    }

    /**
     * @param derivation a fold of a node
     * @param child      a child of the node
     * @return what the child brings into the fold - its own summands if it folds the same key the
     *         same way, else its value - or null if it brings nothing
     */
    private Derivation.Tally summandOf(final Derivation derivation, final StructureObject child) {
        if (!derivation.over.overChildren()) {
            return null;
        }
        final Derivation below = child.derivation(derivation.over.childKeyId);
        if (below != null && derivation.foldsOver(below)) {
            return below.published;
        }
        if (derivation.takes(child)) {
            summand.ofLeaf(child, derivation.over.childKeyId);
            return summand;
        }
        return null;
    }

    /**
     * A child has come under a node: what it brings is added to each fold of the node's children.
     *
     * @param parent the node
     * @param child  the child
     */
    private void foldChildIn(final Node parent, final StructureObject child) {
        final Derivation[] derivations = parent.derivations();
        for (int k = 0; derivations != null && k < derivations.length; k++) {
            final Derivation.Tally brought = summandOf(derivations[k], child);
            if (brought != null) {
                derivations[k].tally.add(brought);
                publish(parent, derivations[k]);
            }
        }
    }

    /**
     * A child has left a node: what it brought is taken out, or the fold is recounted where it
     * cannot be.
     *
     * @param parent the node
     * @param child  the child, still as it was
     */
    private void foldChildOut(final Node parent, final StructureObject child) {
        final Derivation[] derivations = parent.derivations();
        for (int k = 0; derivations != null && k < derivations.length; k++) {
            final Derivation.Tally brought = summandOf(derivations[k], child);
            if (brought != null) {
                if (!derivations[k].tally.replace(brought, nothing)) {
                    retally(parent, derivations[k]);
                }
                publish(parent, derivations[k]);
            }
        }
    }

    /**
     * Writes what an object now derives, and hands the change to every parent above it that
     * folds it in turn: as a difference where the parent sums what it folds, and by recounting
     * otherwise.
     *
     * @param target     the object
     * @param derivation what it derives
     */
    private void publish(final StructureObject target, final Derivation derivation) {
        if (derivation.tally.same(derivation.published)) {
            return;
        }
        writeDerived(target, derivation);
        derivation.previous.set(derivation.published);
        derivation.published.set(derivation.tally);
        final LongSet parents = parentsByChild.get(target.id());
        for (int i = 0; parents != null && i < parents.size(); i++) {
            final StructureObject parent = objectsById.get(parents.valueAt(i));
            final Derivation[] above = parent.derivations();
            for (int k = 0; above != null && k < above.length; k++) {
                if (above[k].foldsOver(derivation)) {
                    if (!above[k].tally.replace(derivation.previous, derivation.published)) {
                        retally(parent, above[k]);
                    }
                    publish(parent, above[k]);
                } else if (above[k].over.childKeyId == derivation.keyId && above[k].takes(target)) {
                    derivedWas.ofTally(derivation.previous);
                    derivedNow.ofTally(derivation.published);
                    if (!above[k].tally.replace(derivedWas, derivedNow)) {
                        retally(parent, above[k]);
                    }
                    publish(parent, above[k]);
                }
            }
        }
    }

    private void writeDerived(final StructureObject target, final Derivation derivation) {
        final PropertyStore properties = target.properties();
        final Derivation.Tally tally = derivation.tally;
        if (writeTally(properties, derivation.keyId, tally)) {
            commitPropertyChange(target, derivation.keyId);
        }
        if (!derivation.over.counts()) {
            return;
        }
        final boolean known = properties.setLong(derivation.knownKeyId, tally.known, NO_SOURCE);
        if (known) {
            commitPropertyChange(target, derivation.knownKeyId);
        }
        final boolean total = properties.setLong(derivation.totalKeyId, tally.total, NO_SOURCE);
        if (total) {
            commitPropertyChange(target, derivation.totalKeyId);
        }
        if (known || total) {
            refoldCounts(target, derivation, known, total);
        }
    }

    /**
     * The counts an object writes are properties a parent may fold. They move only when a
     * summand comes, goes or becomes known, so a fold of them is recounted.
     *
     * @param target     the object
     * @param derivation what it derives
     * @param known      whether how many are known changed
     * @param total      whether how many there are changed
     */
    private void refoldCounts(final StructureObject target,
                              final Derivation derivation,
                              final boolean known,
                              final boolean total) {
        final LongSet parents = parentsByChild.get(target.id());
        for (int i = 0; parents != null && i < parents.size(); i++) {
            final StructureObject parent = objectsById.get(parents.valueAt(i));
            final Derivation[] above = parent.derivations();
            for (int k = 0; above != null && k < above.length; k++) {
                final int read = above[k].over.childKeyId;
                if (above[k].takes(target) && (known && read == derivation.knownKeyId
                        || total && read == derivation.totalKeyId)) {
                    retally(parent, above[k]);
                    publish(parent, above[k]);
                }
            }
        }
    }

    private static boolean writeTally(final PropertyStore properties,
                                      final int keyId,
                                      final Derivation.Tally tally) {
        if (tally.known == 0) {
            return properties.remove(keyId);
        }
        if (tally.doubleCount > 0) {
            return properties.setDouble(keyId, tally.doubleValue(), NO_SOURCE);
        }
        return properties.setLong(keyId, tally.longs, NO_SOURCE);
    }

    /**
     * Cuts a set by a key: a node of the given type for every value the key takes among the
     * objects the selector holds, made when its first member comes and removed when its last
     * goes, with the key's parts as its properties, which only it writes. A part is a path - a
     * property of the member, or {@code from.}, {@code to.} or {@code node.} and a property of the
     * object that intrinsic names, or {@code parent(<axis>)} - the node holding the member on the
     * axis - and perhaps one of its properties; a member missing a part is in no group. What a group folds of its
     * members is declared with {@link #deriveEach(long, int, Fold, Over)}.
     *
     * @param name         what to call it, without ':' and unlike any other grouping; its groups
     *                     are known by {@code <name>:<key>}, the key's parts joined by {@code ,}
     *                     and a text that could be taken for a number or a flag quoted; while it
     *                     lives no other object takes an id under its name
     * @param type         the type of the nodes it makes
     * @param selectorText what it holds, e.g. {@code link[type=flow]}
     * @param by           the key's parts, e.g. {@code from.node}, {@code to.node}, {@code from.family}
     * @return the grouping
     */
    public Grouping groupBy(final String name,
                            final String type,
                            final String selectorText,
                            final String... by) {
        requireTask();
        if (isBlank(name) || isBlank(type) || by == null || by.length == 0) {
            throw new IllegalArgumentException("name, type and a key are required");
        }
        requireFreeGroupingName(name);
        final MemberPath[] paths = new MemberPath[by.length];
        final int[] partKeyIds = new int[by.length];
        for (int i = 0; i < by.length; i++) {
            paths[i] = MemberPath.parse(by[i], propertyKeys);
            partKeyIds[i] = propertyKeys.idOf(by[i]);
        }
        final Grouping grouping = new Grouping(globalIdCounter.getAndIncrement(), name, type,
                selectorText, by, Selector.parse(selectorText, propertyKeys, Structure.this::parentOn),
                paths, partKeyIds);
        final Grouping[] grown = Arrays.copyOf(groupings, groupings.length + 1);
        grown[groupings.length] = grouping;
        groupings = grown;
        groupingsById.put(grouping.id(), grouping);
        groupingsByName.put(name, grouping);
        final List<StructureObject> everything = new ArrayList<>(objectsById.values());
        for (final StructureObject object : everything) {
            regroup(grouping, object);
        }
        return grouping;
    }

    /**
     * Declares a property of every group of a grouping, there now and to come, the fold of
     * {@code over} - {@link Over#members(String)} for its members. Runs inside a task.
     *
     * @param groupingId the grouping
     * @param keyId      what to write on each group
     * @param fold       how the summands come to one value
     * @param over       what the summands are
     */
    public void deriveEach(final long groupingId, final int keyId, final Fold fold, final Over over) {
        requireTask();
        requireFoldable(keyId, fold, over);
        final Grouping grouping = requireGrouping(groupingId);
        requireOutsideKey(grouping, keyId, over);     // for groups to come as well
        for (final Grouping.Spec held : grouping.derivations()) {
            if (held.keyId != keyId) {
                continue;
            }
            if (held.fold == fold && held.over.equals(over)) {
                return;
            }
            throw new IllegalArgumentException("Property '" + propertyKeys.nameOf(keyId)
                    + "' of the groups of '" + grouping.name() + "' is already derived otherwise");
        }
        final long[] groupIds = grouping.groupIds();
        for (int i = 0; i < groupIds.length; i++) {
            requireDerivable(objectsById.get(groupIds[i]), keyId, fold, over);
        }
        grouping.addDerivation(new Grouping.Spec(keyId, fold, over),
                over.overMembers() ? MemberPath.parse(over.memberPath, propertyKeys) : null);
        for (int i = 0; i < groupIds.length; i++) {
            derive(groupIds[i], keyId, fold, over);
        }
    }

    /**
     * @param groupingId the grouping
     * @return the ids of the groups it holds now
     */
    public CompletableFuture<long[]> groups(final long groupingId) {
        return submit(() -> requireGrouping(groupingId).groupIds());
    }

    /**
     * Stops a grouping and takes its groups away.
     *
     * @param groupingId the grouping
     */
    public void removeGrouping(final long groupingId) {
        requireTask();
        final Grouping grouping = groupingsById.remove(groupingId);
        if (grouping == null) {
            return;
        }
        final Grouping[] held = groupings;
        final Grouping[] shrunk = new Grouping[held.length - 1];
        for (int i = 0, at = 0; i < held.length; i++) {
            if (held[i] != grouping) {
                shrunk[at++] = held[i];
            }
        }
        groupings = shrunk;
        groupingsByName.remove(grouping.name());
        final long[] groupIds = grouping.groupIds();
        for (int i = 0; i < groupIds.length; i++) {
            groupingOfNode.remove(groupIds[i]);
            removeOnWorker(groupIds[i]);
        }
    }

    /**
     * @param name what a new grouping would be called
     */
    private void requireFreeGroupingName(final String name) {
        if (name.indexOf(':') >= 0) {
            throw new IllegalArgumentException("A grouping name has no ':': " + name);
        }
        if (groupingsByName.get(name) != null) {
            throw new IllegalArgumentException("A grouping named '" + name + "' is already there");
        }
        for (int i = 0; i < objectsByExternalId.size(); i++) {
            final CharSequence held = objectsByExternalId.keyAt(i);
            if (held.length() > name.length() && held.charAt(name.length()) == ':'
                    && name.contentEquals(held.subSequence(0, name.length()))) {
                throw new IllegalArgumentException("External id '" + held
                        + "' is already under the name '" + name + "'");
            }
        }
    }

    /**
     * @param objectId an object
     * @return whether a grouping made it
     */
    boolean isGroup(final long objectId) {
        return groupingOfNode.get(objectId) != null;
    }

    private Grouping requireGrouping(final long groupingId) {
        final Grouping grouping = groupingsById.get(groupingId);
        if (grouping == null) {
            throw new NoSuchElementException("No such grouping: " + groupingId);
        }
        return grouping;
    }

    /**
     * @param member the object the path starts at
     * @param path   where to read
     * @return the object holding the path's key: the member, its parent on an axis or the one
     *         its intrinsic id names; null if there is none
     */
    private StructureObject reach(final StructureObject member, final MemberPath path) {
        switch (path.hop) {
            case SELF:
                return member;
            case PARENT:
                return parentOn(member.id(), path.axis);
            default:
                final int hop = path.hop.keyId;
                return member.valueTypeOf(hop) == ValueType.LONG
                        ? objectsById.get(member.longValueOf(hop)) : null;
        }
    }

    /**
     * @param grouping what cuts
     * @param member   what is cut
     * @return whether the member has a key - then in {@link #groupKey} - or misses a part
     */
    private boolean keyOf(final Grouping grouping, final StructureObject member) {
        groupKey.setLength(0);
        for (int i = 0; i < grouping.by.length; i++) {
            final MemberPath path = grouping.by[i];
            final StructureObject holder = reach(member, path);
            final int keyId = path.keyId;
            final ValueType valueType = holder != null ? holder.valueTypeOf(keyId) : ValueType.ABSENT;
            if (valueType == ValueType.ABSENT) {
                return false;
            }
            if (i > 0) {
                groupKey.append(',');
            }
            switch (valueType) {
                case LONG:
                    groupKey.append(holder.longValueOf(keyId));
                    break;
                case DOUBLE:
                    groupKey.append(holder.doubleValueOf(keyId));
                    break;
                case BOOLEAN:
                    groupKey.append(holder.booleanValueOf(keyId));
                    break;
                default:
                    final CharSequence text = holder.textValueOf(keyId);
                    final boolean quoted = readsAsValue(text);
                    if (quoted) {
                        groupKey.append('"');
                    }
                    for (int k = 0; k < text.length(); k++) {
                        final char c = text.charAt(k);
                        if (c == ',' || c == '\\' || c == '"') {
                            groupKey.append('\\');
                        }
                        groupKey.append(c);
                    }
                    if (quoted) {
                        groupKey.append('"');
                    }
                    break;
            }
        }
        return true;
    }

    /**
     * @param text a text part of a key
     * @return whether it could be taken for a number or a flag written into a key, and so is
     *         quoted there
     */
    private static boolean readsAsValue(final CharSequence text) {
        final int first = text.length() > 1 && text.charAt(0) == '-' ? 1 : 0;
        return text.length() > first && Character.isDigit(text.charAt(first))
                || TextObjectMap.sameText("true", text) || TextObjectMap.sameText("false", text)
                || TextObjectMap.sameText("NaN", text) || TextObjectMap.sameText("Infinity", text)
                || TextObjectMap.sameText("-Infinity", text);
    }

    /**
     * Puts an object where its key says now: in the same group again, which folds it again; in
     * another, made if it is the first there; or in none. A group is no member of anything.
     *
     * @param grouping what cuts
     * @param object   what may be a member
     */
    private void regroup(final Grouping grouping, final StructureObject object) {
        if (groupingOfNode.get(object.id()) != null || objectsById.get(object.id()) != object) {
            return;
        }
        final boolean keyed = grouping.selector.test(object) && keyOf(grouping, object);
        final String was = grouping.keyOfMember(object.id());
        if (keyed && was != null && TextObjectMap.sameText(was, groupKey)) {
            foldIn(objectsById.get(grouping.groupOf(was)), object);
            return;
        }
        final Long into = keyed ? grouping.groupOf(groupKey) : null;
        final String fresh = keyed && into == null ? groupKey.toString() : null;   // only a new group's key
        if (was != null) {
            leaveGroup(grouping, object.id(), was);
        }
        if (into != null) {
            final StructureObject group = objectsById.get(into);
            grouping.join(object.id(), group.name(), into);
            foldIn(group, object);
        } else if (fresh != null) {
            makeGroup(grouping, object, fresh);
        }
    }

    private void makeGroup(final Grouping grouping, final StructureObject member, final String key) {
        final long id = globalIdCounter.getAndIncrement();
        final Node group = new Node(id, key, grouping.type(), grouping.name() + ':' + key);
        groupingOfNode.put(id, grouping);
        grouping.addGroup(key, id);
        grouping.join(member.id(), key, id);
        registerNewObject(group);
        for (int i = 0; i < grouping.by.length; i++) {
            final MemberPath path = grouping.by[i];
            final StructureObject holder = reach(member, path);
            final int from = path.keyId;
            final int keyId = grouping.partKeyIds[i];
            final PropertyStore properties = group.properties();
            final boolean written;
            switch (holder.valueTypeOf(from)) {
                case LONG:
                    written = properties.setLong(keyId, holder.longValueOf(from), NO_SOURCE);
                    break;
                case DOUBLE:
                    written = properties.setDouble(keyId, holder.doubleValueOf(from), NO_SOURCE);
                    break;
                case BOOLEAN:
                    written = properties.setBoolean(keyId, holder.booleanValueOf(from), NO_SOURCE);
                    break;
                default:
                    written = properties.setText(keyId, holder.textValueOf(from).toString(), NO_SOURCE);
                    break;
            }
            if (written) {
                commitPropertyChange(group, keyId);
            }
        }
        for (final Grouping.Spec spec : grouping.derivations()) {
            derive(id, spec.keyId, spec.fold, spec.over);
        }
    }

    private void leaveGroup(final Grouping grouping, final long memberId, final String key) {
        final long groupId = grouping.leave(memberId);
        if (groupId < 0) {
            return;
        }
        if (grouping.membersOf(groupId).size() > 0) {
            foldOut(objectsById.get(groupId), memberId);
            return;
        }
        grouping.removeGroup(key, groupId);
        groupingOfNode.remove(groupId);
        removeOnWorker(groupId);
    }

    private void ungroupAll(final StructureObject removed) {
        final Grouping[] held = groupings;
        for (int i = 0; i < held.length; i++) {
            final String was = held[i].keyOfMember(removed.id());
            if (was != null) {
                leaveGroup(held[i], removed.id(), was);
            }
        }
    }

    /**
     * Puts a node under another on an axis: a silo holds its components, a component what it
     * runs on. A node holds children on one axis, the one its first child is put on; a child
     * has at most one parent per axis, and no node is under itself, on any mix of axes.
     * Putting it where it is already is nothing.
     *
     * @param parentId the node that holds
     * @param childId  the node held
     * @param axis     the axis
     */
    public void contain(final long parentId, final long childId, final String axis) {
        requireTask();
        if (axis == null || axis.isBlank()) {
            throw new IllegalArgumentException("axis must not be null or blank");
        }
        final Node parent = (Node) requireObject(parentId, ObjectKind.NODE);
        final Node child = (Node) requireObject(childId, ObjectKind.NODE);
        if (!parent.canHoldOn(axis)) {
            throw new IllegalArgumentException("'" + parentId + "' holds on axis '"
                    + parent.axis() + "', not '" + axis + "'");
        }
        if (parent.childrenMap().get(child.id()) != null) {
            return;
        }
        if (isAtOrAbove(child.id(), parent.id())) {
            throw new IllegalArgumentException(
                    "Cycle detected: cannot place '" + childId + "' inside '" + parentId + "'");
        }
        rejectSecondParentOnAxis(child.id(), parent.id(), axis);
        final boolean holding = parent.axis() == null;
        parent.holdOn(axis);
        parent.childrenMap().put(child.id(), child);
        indexMemberAdd(child.id(), parent.id());
        versionCounter.incrementAndGet();
        if (holding) {
            markDirtyInAllViews(parent, PropertyKeys.AXIS);
        }
        emitMembership(child, parent, ChangeKind.CONTAINED);
        if (holding) {
            recheck(parent, PropertyKeys.AXIS);    // after the pair: a parent entering a view states it once
        }
        foldChildIn(parent, child);
    }

    /**
     * A cycle through two axes is as endless to place and to fold as one through a single axis.
     *
     * @param candidateId the node that would go under
     * @param nodeId      the node it would go under
     * @return whether the candidate is that node or holds it, on any mix of axes
     */
    private boolean isAtOrAbove(final long candidateId, final long nodeId) {
        ancestorsSeen.clear();
        int depth = 0;
        ancestorsToVisit[depth++] = nodeId;
        while (depth > 0) {
            final long at = ancestorsToVisit[--depth];
            if (at == candidateId) {
                return true;
            }
            final LongSet parentIds = parentsByChild.get(at);
            for (int i = 0; parentIds != null && i < parentIds.size(); i++) {
                final long parentId = parentIds.valueAt(i);
                if (ancestorsSeen.add(parentId)) {
                    if (depth == ancestorsToVisit.length) {
                        ancestorsToVisit = Arrays.copyOf(ancestorsToVisit, depth * 2);
                    }
                    ancestorsToVisit[depth++] = parentId;
                }
            }
        }
        return false;
    }

    /**
     * Takes a node from under another; nothing if it is not there.
     *
     * @param parentId the node that held it
     * @param childId  the node
     */
    public void uncontain(final long parentId, final long childId) {
        requireTask();
        final Node parent = (Node) requireObject(parentId, ObjectKind.NODE);
        final Node child = parent.childrenMap().remove(childId);
        if (child != null) {
            indexMemberRemove(childId, parent.id());
            versionCounter.incrementAndGet();
            emitLeftIfPresent(childId, parent);
            foldChildOut(parent, child);
        }
    }

    public void retargetLinkFrom(final long linkId,
                                 final long newOutputId) {
        requireTask();
        final Link link = (Link) requireObject(linkId, ObjectKind.LINK);
        requireNotDerived(link);
        final Output newOutput =
                (Output) requireObject(newOutputId, ObjectKind.OUTPUT);
        final Output oldOutput = link.fromOutput();
        if (oldOutput != null) {
            unindexLinkEnd(linksByFromOutputId, oldOutput, link.id());
        }
        link.retargetFrom(newOutput);
        addToIndex(linksByFromOutputId, newOutput.id(), link.id());
        newOutput.addLinks(1);
        versionCounter.incrementAndGet();
        markDirtyInAllViews(link, PropertyKeys.FROM);
        recheck(link);
        refreshPortInAllViews(oldOutput);
        refreshPortInAllViews(newOutput);
        restateLinksOf(oldOutput);
        restateLinksOf(newOutput);
    }

    public void retargetLinkTo(final long linkId,
                               final long newInputId) {
        requireTask();
        final Link link = (Link) requireObject(linkId, ObjectKind.LINK);
        requireNotDerived(link);
        final Input newInput =
                (Input) requireObject(newInputId, ObjectKind.INPUT);
        final Input oldInput = link.toInput();
        if (oldInput != null) {
            unindexLinkEnd(linksByToInputId, oldInput, link.id());
        }
        link.retargetTo(newInput);
        addToIndex(linksByToInputId, newInput.id(), link.id());
        newInput.addLinks(1);
        versionCounter.incrementAndGet();
        markDirtyInAllViews(link, PropertyKeys.TO);
        recheck(link);
        refreshPortInAllViews(oldInput);
        refreshPortInAllViews(newInput);
        restateLinksOf(oldInput);
        restateLinksOf(newInput);
    }

    public CompletableFuture<Void> setViewSelector(final long viewId,
                                                   final String newSelectorText) {
        return run(new Runnable() {
            @Override
            public void run() {
                final View view = requireView(viewId);
                view.replaceSelector(newSelectorText);
                versionCounter.incrementAndGet();
                rebuildViewMembership(view);
                view.restateToAll();
            }
        });
    }

    public CompletableFuture<Void> setViewPropertyKeys(final long viewId,
                                                       final Set<String> newKeys) {
        return run(new Runnable() {
            @Override
            public void run() {
                final View view = requireView(viewId);
                view.replacePropertyKeys(newKeys);
                versionCounter.incrementAndGet();
                view.restateToAll();
            }
        });
    }

    public CompletableFuture<Void> setViewDeliveryPolicy(final long viewId,
                                                         final DeliveryPolicy newPolicy) {
        return run(new Runnable() {
            @Override
            public void run() {
                if (newPolicy == null) {
                    throw new IllegalArgumentException("newPolicy is required");
                }
                final View view = requireView(viewId);
                view.replaceDeliveryPolicy(newPolicy);
                versionCounter.incrementAndGet();
            }
        });
    }

    public void remove(final long objectId) {
        requireTask();
        final Grouping grouping = groupingOfNode.get(objectId);
        if (grouping != null) {
            throw new IllegalArgumentException("Object " + objectId + " is a group of '" + grouping.name()
                    + "'; it goes with its last member or with the grouping");
        }
        removeOnWorker(objectId);
    }

    private void removeOnWorker(final long objectId) {
        final StructureObject target = objectsById.get(objectId);
        if (target == null) {
            return;
        }
        if (target instanceof Link) {
            requireNotDerived((Link) target);
        }

        if (target instanceof Node) {
            final Node node = (Node) target;
            final List<Input> inputsCopy = new ArrayList<>(node.inputs());
            final List<Output> outputsCopy = new ArrayList<>(node.outputs());
            for (final Input input : inputsCopy) {
                cascadeRemoveLinksToInput(input);
                cascadeRemoveSingle(input);
            }
            for (final Output output : outputsCopy) {
                cascadeRemoveLinksFromOutput(output);
                cascadeRemoveSingle(output);
            }
        } else if (target instanceof Input) {
            cascadeRemoveLinksToInput((Input) target);
        } else if (target instanceof Output) {
            cascadeRemoveLinksFromOutput((Output) target);
        }
        if (target instanceof Node) {
            releaseChildrenOf((Node) target);
        }

        if (target instanceof Input) {
            final Input input = (Input) target;
            input.owningNode().inputsMap().remove(input.id());
        } else if (target instanceof Output) {
            final Output output = (Output) target;
            output.owningNode().outputsMap().remove(output.id());
        }

        detachFromParents(target);

        if (target instanceof Link) {
            indexLinkRemove((Link) target);
        }
        forgetDerivations(target);
        objectsById.remove(target.id());
        forgetExternalId(target);
        forgetAddress(target);
        versionCounter.incrementAndGet();
        emitRemoved(target);
        if (target instanceof Link) {
            restateLinks((Link) target);
        }
    }

    private void cascadeRemoveSingle(final StructureObject child) {
        detachFromParents(child);
        if (child instanceof Link) {
            indexLinkRemove((Link) child);
        }
        forgetDerivations(child);
        objectsById.remove(child.id());
        forgetExternalId(child);
        forgetAddress(child);
        emitRemoved(child);
        if (child instanceof Link) {
            restateLinks((Link) child);
        }
    }

    private void cascadeRemoveLinksToInput(final Input removedInput) {
        final LongSet linkIds = linksByToInputId.remove(removedInput.id());
        if (linkIds == null) {
            return;
        }
        for (int i = 0; i < linkIds.size(); i++) {
            final StructureObject candidate = objectsById.get(linkIds.valueAt(i));
            if (candidate instanceof Link) {
                final Link link = (Link) candidate;
                unindexLinkEnd(linksByFromOutputId, link.fromOutput(), link.id());
                cascadeRemoveSingle(link);
            }
        }
    }

    private void cascadeRemoveLinksFromOutput(final Output removedOutput) {
        final LongSet linkIds = linksByFromOutputId.remove(removedOutput.id());
        if (linkIds == null) {
            return;
        }
        for (int i = 0; i < linkIds.size(); i++) {
            final StructureObject candidate = objectsById.get(linkIds.valueAt(i));
            if (candidate instanceof Link) {
                final Link link = (Link) candidate;
                unindexLinkEnd(linksByToInputId, link.toInput(), link.id());
                cascadeRemoveSingle(link);
            }
        }
    }

    /**
     * Detaches {@code target} from every parent that directly contains it, in O(degree) of the
     * target rather than O(N) over the structure.
     *
     * @param target object to detach
     */
    private void detachFromParents(final StructureObject target) {
        final long targetId = target.id();
        final LongSet parentIds = parentsByChild.remove(targetId);
        if (parentIds == null) {
            return;
        }
        for (int i = 0; i < parentIds.size(); i++) {
            final StructureObject candidate = objectsById.get(parentIds.valueAt(i));
            if (candidate instanceof Node) {
                ((Node) candidate).removeChild(targetId);
                foldChildOut((Node) candidate, target);
            }
        }
    }

    /**
     * Membership is one statement, made from the member's side, and it reaches a view only
     * where the view holds both ends: a parent the view does not deliver is not somewhere it
     * could show anything.
     *
     * @param member     the object placed or displaced
     * @param parent      the parent
     * @param changeKind which of the two happened
     */
    private void emitMembership(final StructureObject member,
                                final Node parent,
                                final ChangeKind changeKind) {
        final View[] snapshot = views;
        for (int i = 0; i < snapshot.length; i++) {
            final View view = snapshot[i];
            if (view.matchedObjectIds().contains(member.id())
                    && view.matchedObjectIds().contains(parent.id())) {
                view.enqueueMembership(changeKind, member, parent.id());
            }
        }
        placed(member);
    }

    /**
     * An object moved on an axis: a watcher reading placement may now hold it, and whatever it
     * holds, otherwise; one reading the parent, the object itself.
     *
     * @param moved the object that joined or left a parent
     */
    private void placed(final StructureObject moved) {
        final View[] heldViews = views;
        for (int i = 0; i < heldViews.length; i++) {
            placed(heldViews[i], moved);
        }
        final Grouping[] heldGroupings = groupings;
        for (int i = 0; i < heldGroupings.length; i++) {
            placed(heldGroupings[i], moved);
        }
    }

    private void placed(final Watcher watcher, final StructureObject moved) {
        if (watcher.reaches(Watcher.PLACEMENT)) {
            recheckBelow(watcher, moved);
        } else if (watcher.reaches(Watcher.PARENT)) {
            recheckOne(watcher, moved);
        }
    }

    private void recheckBelow(final Watcher watcher, final StructureObject object) {
        recheckAround(watcher, object);
        if (object instanceof Node) {
            final LongObjectMap<Node> children = ((Node) object).childrenMap();
            for (int i = 0; i < children.size(); i++) {
                recheckBelow(watcher, children.valueAt(i));
            }
        }
    }

    private void emitLeftIfPresent(final long childId, final Node parent) {
        final StructureObject member = objectsById.get(childId);
        if (member != null) {
            emitMembership(member, parent, ChangeKind.UNCONTAINED);
        }
    }

    public CompletableFuture<Map<String, Object>> snapshotObject(final long objectId) {
        return submit(new Supplier<Map<String, Object>>() {
            @Override
            public Map<String, Object> get() {
                return requireObject(objectId).propertiesSnapshot(propertyKeys);
            }
        });
    }

    /**
     * @param linkId the link
     * @return {@code {fromOutputId, fromNodeId, toInputId, toNodeId}}
     */
    public CompletableFuture<long[]> linkEndpoints(final long linkId) {
        return submit(new Supplier<long[]>() {
            @Override
            public long[] get() {
                final Link link = (Link) requireObject(linkId, ObjectKind.LINK);
                return new long[]{
                        link.fromOutput().id(),
                        link.fromOutput().owningNode().id(),
                        link.toInput().id(),
                        link.toInput().owningNode().id()};
            }
        });
    }

    /**
     * @param nodeId   the node
     * @param portKind {@link ObjectKind#INPUT} or {@link ObjectKind#OUTPUT}
     * @return ids of the node's ports of that kind
     */
    public CompletableFuture<long[]> nodePorts(final long nodeId, final ObjectKind portKind) {
        return submit(new Supplier<long[]>() {
            @Override
            public long[] get() {
                final Node node = (Node) requireObject(nodeId, ObjectKind.NODE);
                final Collection<? extends StructureObject> ports =
                        portKind == ObjectKind.INPUT ? node.inputs() : node.outputs();
                return idsOf(ports);
            }
        });
    }

    /**
     * @param parentId the parent
     * @return ids of its direct members, members before nested parents; a parent has one axis, so
     *         this is the whole of what it holds on that axis
     */
    public CompletableFuture<long[]> children(final long parentId) {
        return submit(new Supplier<long[]>() {
            @Override
            public long[] get() {
                final StructureObject candidate = requireObject(parentId);
                if (!(candidate instanceof Node)) {
                    throw new IllegalArgumentException(
                            "Object '" + parentId + "' is " + candidate.kind() + ", expected a parent");
                }
                return ((Node) candidate).childIds();
            }
        });
    }

    /**
     * @param axis the axis the path runs along
     * @param path segments from a root of the axis down
     * @return the id of the object at the path, or -1 if there is none; fails if a segment by
     *         name alone matches more than one object
     */
    public CompletableFuture<Long> resolve(final String axis, final Path path) {
        return submit(() -> {
            StructureObject at = null;
            for (int i = 0; i < path.length(); i++) {
                if (i > 0 && !(at instanceof Node)) {
                    return -1L;
                }
                at = i == 0 ? rootAt(axis, path) : memberAt((Node) at, axis, path, i);
                if (at == null) {
                    return -1L;
                }
            }
            return at != null ? at.id() : -1L;
        });
    }

    /**
     * @param objectId the object
     * @param axis     the axis
     * @return its path on the axis by typed names, or null if it is not on the axis
     */
    public CompletableFuture<Path> pathOf(final long objectId, final String axis) {
        return submit(() -> {
            StructureObject at = requireObject(objectId);
            final boolean onAxis = at instanceof Node && axis.equals(((Node) at).axis());
            if (!onAxis && parentOn(at.id(), axis) == null) {
                return null;
            }
            final List<StructureObject> up = new ArrayList<>();
            for (; at != null; at = parentOn(at.id(), axis)) {
                up.add(at);
            }
            Path path = Path.root();
            for (int i = up.size() - 1; i >= 0; i--) {
                final StructureObject segment = up.get(i);
                path = segment.name() != null && !segment.name().isEmpty()
                        ? path.child(segment.type(), segment.name()) : path.child(segment.id());
            }
            return path;
        });
    }

    /**
     * @param childId an object
     * @param axis     an axis
     * @return the parent holding the object on the axis, or null
     */
    Node parentOn(final long childId, final String axis) {
        final LongSet parentIds = parentsByChild.get(childId);
        for (int i = 0; parentIds != null && i < parentIds.size(); i++) {
            final StructureObject parent = objectsById.get(parentIds.valueAt(i));
            if (parent instanceof Node && axis.equals(((Node) parent).axis())) {
                return (Node) parent;
            }
        }
        return null;
    }

    private StructureObject rootAt(final String axis, final Path path) {
        StructureObject found = null;
        for (int i = 0; i < objectsById.size(); i++) {
            final StructureObject candidate = objectsById.valueAt(i);
            if (candidate instanceof Node && axis.equals(((Node) candidate).axis())
                    && parentOn(candidate.id(), axis) == null && path.matchesAt(0, candidate)) {
                found = onlyOne(found, candidate, axis, path, 0);
            }
        }
        return found;
    }

    private StructureObject memberAt(final Node parent,
                                     final String axis,
                                     final Path path,
                                     final int index) {
        final LongObjectMap<Node> children = parent.childrenMap();
        StructureObject found = null;
        for (int i = 0; i < children.size(); i++) {
            final StructureObject candidate = children.valueAt(i);
            if (path.matchesAt(index, candidate)) {
                found = onlyOne(found, candidate, axis, path, index);
            }
        }
        return found;
    }

    private static StructureObject onlyOne(final StructureObject found,
                                           final StructureObject candidate,
                                           final String axis,
                                           final Path path,
                                           final int index) {
        if (found == null) {
            return candidate;
        }
        throw new IllegalArgumentException("Segment " + (index + 1) + " of " + path
                + " on axis '" + axis + "' is both #" + found.id() + " of type '" + found.type()
                + "' and #" + candidate.id() + " of type '" + candidate.type() + "'; name the type");
    }

    /**
     * @param objectId the object
     * @return ids of the parents directly holding it, one per axis it is placed on
     */
    public CompletableFuture<long[]> parents(final long objectId) {
        return submit(new Supplier<long[]>() {
            @Override
            public long[] get() {
                final LongSet parentIds = parentsByChild.get(objectId);
                if (parentIds == null) {
                    return new long[0];
                }
                final long[] result = new long[parentIds.size()];
                for (int i = 0; i < parentIds.size(); i++) {
                    result[i] = parentIds.valueAt(i);
                }
                return result;
            }
        });
    }

    private static long[] idsOf(final Collection<? extends StructureObject> objects) {
        final long[] result = new long[objects.size()];
        int at = 0;
        for (final StructureObject object : objects) {
            result[at++] = object.id();
        }
        return result;
    }

    public CompletableFuture<long[]> matchedObjectIds(final String selectorText) {
        return submit(new Supplier<long[]>() {
            @Override
            public long[] get() {
                final Selector.Expression expression =
                        Selector.parse(selectorText, propertyKeys, Structure.this::parentOn);
                long[] matched = new long[16];
                int count = 0;
                for (int i = 0; i < objectsById.size(); i++) {
                    final StructureObject candidate = objectsById.valueAt(i);
                    if (expression.mayMatch(candidate.kind()) && expression.test(candidate)) {
                        if (count == matched.length) {
                            matched = Arrays.copyOf(matched, count * 2);
                        }
                        matched[count++] = candidate.id();
                    }
                }
                return Arrays.copyOf(matched, count);
            }
        });
    }

    public CompletableFuture<List<Map<String, Object>>> query(final String selectorText) {
        return submit(new Supplier<List<Map<String, Object>>>() {
            @Override
            public List<Map<String, Object>> get() {
                final Selector.Expression expression =
                        Selector.parse(selectorText, propertyKeys, Structure.this::parentOn);
                final List<Map<String, Object>> results = new ArrayList<>();
                for (int i = 0; i < objectsById.size(); i++) {
                    final StructureObject candidate = objectsById.valueAt(i);
                    if (expression.mayMatch(candidate.kind()) && expression.test(candidate)) {
                        results.add(candidate.propertiesSnapshot(propertyKeys));
                    }
                }
                return results;
            }
        });
    }

    public CompletableFuture<Runnable> subscribe(final long viewId,
                                                 final BatchSubscriber subscriber) {
        if (subscriber == null) {
            throw new IllegalArgumentException("subscriber must not be null");
        }
        return submit(new Supplier<Runnable>() {
            @Override
            public Runnable get() {
                final View view = requireView(viewId);
                view.attach(subscriber);
                return new Runnable() {
                    @Override
                    public void run() {
                        try {
                            worker.execute(new Runnable() {
                                @Override
                                public void run() {
                                    view.detach(subscriber);
                                }
                            });
                        } catch (final RejectedExecutionException ignored) {
                        }
                    }
                };
            }
        });
    }

    /**
     * Re-states a view to one subscriber: what the view owes everyone goes first, then a
     * complete snapshot built from current state. What a session that has fallen behind asks for.
     *
     * @param viewId     view to re-state
     * @param subscriber subscriber to deliver the snapshot to
     * @return future completed once the snapshot is owed; it is built and handed over as soon as
     *         the delivery ring has room
     */
    public CompletableFuture<Void> snapshotView(final long viewId,
                                                final BatchSubscriber subscriber) {
        return run(new Runnable() {
            @Override
            public void run() {
                requireView(viewId).restateTo(subscriber);
            }
        });
    }

    private void registerNewObject(final StructureObject newObject) {
        objectsById.put(newObject.id(), newObject);
        indexExternalId(newObject);
        final int wanted = demandedLevel(newObject.id());
        if (newObject.requireLevel(wanted)) {
            announceDemand(newObject, wanted);   // a source takes what it is not told for whole
        }
        versionCounter.incrementAndGet();
        recheck(newObject);
    }

    private void emitRemoved(final StructureObject target) {
        ungroupAll(target);
        final View[] snapshot = views;
        for (int i = 0; i < snapshot.length; i++) {
            final View view = snapshot[i];
            view.selectorMatched().remove(target.id());
            if (view.matchedObjectIds().remove(target.id())) {
                view.enqueueStructural(ChangeKind.REMOVED, target);
                if (target instanceof Link) {
                    refreshPortsOf(view, (Link) target);
                }
            }
        }
    }

    private void markDirtyInAllViews(final StructureObject target, final int keyId) {
        final View[] snapshot = views;
        for (int i = 0; i < snapshot.length; i++) {
            final View view = snapshot[i];
            if (view.matchedObjectIds().contains(target.id())
                    && view.isInterestedInKey(keyId)) {
                view.markDirty(target.id(), keyId);
            }
        }
    }

    private void rebuildViewMembership(final View view) {
        view.clearMembership();
        final Selector.Expression selector = view.selectorExpression();
        for (int i = 0; i < objectsById.size(); i++) {
            final StructureObject candidate = objectsById.valueAt(i);
            if (selector.mayMatch(candidate.kind()) && selector.test(candidate)) {
                view.selectorMatched().add(candidate.id());
                view.addMember(candidate.id());
            }
        }
        for (int i = 0; i < objectsById.size(); i++) {
            final StructureObject candidate = objectsById.valueAt(i);
            if (candidate instanceof Port && carriesPort(view, (Port) candidate)) {
                view.addMember(candidate.id());
            }
        }
    }

    private void recheck(final StructureObject changed) {
        recheck(changed, ANY_KEY);
    }

    /**
     * Asks every watcher whether a change of one object changes what it holds: the object, and
     * what reads it - the links at a node or a port, the ports of a node, the children of a node.
     *
     * @param changed the object
     * @param keyId   the property that changed, or {@link #ANY_KEY} when more than a property
     *                did; a watcher that reads the property nowhere is not asked
     */
    private void recheck(final StructureObject changed, final int keyId) {
        final View[] heldViews = views;
        for (int i = 0; i < heldViews.length; i++) {
            if (keyId == ANY_KEY || heldViews[i].reads(keyId)) {
                recheckAround(heldViews[i], changed);
            }
        }
        final Grouping[] heldGroupings = groupings;
        for (int i = 0; i < heldGroupings.length; i++) {
            if (keyId == ANY_KEY || heldGroupings[i].reads(keyId)) {
                recheckAround(heldGroupings[i], changed);
            }
        }
    }

    private void recheckAround(final Watcher watcher, final StructureObject changed) {
        recheckOne(watcher, changed);
        if (changed instanceof Port) {
            if (watcher.reaches(Watcher.PORTS)) {
                recheckLinks(watcher, linksAt((Port) changed));
            }
            return;
        }
        if (!(changed instanceof Node)) {
            return;
        }
        final Node node = (Node) changed;
        final boolean ends = watcher.reaches(Watcher.ENDS);
        final boolean owner = watcher.reaches(Watcher.OWNER);
        if (ends || owner) {
            final LongObjectMap<Input> inputs = node.inputsMap();
            for (int i = 0; i < inputs.size(); i++) {
                if (owner) {
                    recheckOne(watcher, inputs.valueAt(i));
                }
                if (ends) {
                    recheckLinks(watcher, linksByToInputId.get(inputs.keyAt(i)));
                }
            }
            final LongObjectMap<Output> outputs = node.outputsMap();
            for (int i = 0; i < outputs.size(); i++) {
                if (owner) {
                    recheckOne(watcher, outputs.valueAt(i));
                }
                if (ends) {
                    recheckLinks(watcher, linksByFromOutputId.get(outputs.keyAt(i)));
                }
            }
        }
        if (watcher.reaches(Watcher.PARENT)) {
            final LongObjectMap<Node> children = node.childrenMap();
            for (int i = 0; i < children.size(); i++) {
                recheckOne(watcher, children.valueAt(i));
            }
        }
    }

    private void recheckLinks(final Watcher watcher, final LongSet linkIds) {
        for (int i = 0; linkIds != null && i < linkIds.size(); i++) {
            final StructureObject link = objectsById.get(linkIds.valueAt(i));
            if (link != null) {
                recheckOne(watcher, link);
            }
        }
    }

    private void recheckOne(final Watcher watcher, final StructureObject object) {
        if (watcher instanceof View) {
            reEvaluate((View) watcher, object);
        } else {
            regroup((Grouping) watcher, object);
        }
    }

    private void reEvaluate(final View view, final StructureObject changed) {
        if (view.selectorExpression().test(changed)) {
            view.selectorMatched().add(changed.id());
        } else {
            view.selectorMatched().remove(changed.id());
        }
        applyMembership(view, changed);
    }

    /**
     * Brings the delivered membership of one object in line with the selector - and a port's
     * with the links delivered at it - emitting the addition or the removal to the view.
     *
     * @param view   view to update
     * @param object object whose membership is in question
     */
    private void applyMembership(final View view, final StructureObject object) {
        final boolean belongs = view.selectorMatched().contains(object.id())
                || (object instanceof Port && carriesPort(view, (Port) object));
        final boolean delivered = view.matchedObjectIds().contains(object.id());
        if (belongs == delivered) {
            return;
        }
        if (belongs) {
            view.addMember(object.id());
            view.enqueueStructural(ChangeKind.ADDED, object);
            view.markEveryPropertyDirty(object);
            enqueueMembershipsOf(view, object);
        } else {
            view.removeMember(object.id());
            view.enqueueStructural(ChangeKind.REMOVED, object);
        }
        if (object instanceof Link) {
            refreshPortsOf(view, (Link) object);
        }
    }

    /**
     * States the memberships of an object that has just entered a view - both the parents it
     * is in and, if it is a parent itself, the members it holds. What is already delivered on
     * the other side is stated here, so a pair is stated exactly once however it came about.
     *
     * @param view   the view the object entered
     * @param object the object
     */
    private void enqueueMembershipsOf(final View view, final StructureObject object) {
        final LongSet parentIds = parentsOf(object.id());
        if (parentIds != null) {
            for (int i = 0; i < parentIds.size(); i++) {
                final long parentId = parentIds.valueAt(i);
                if (view.matchedObjectIds().contains(parentId)) {
                    view.enqueueMembership(ChangeKind.CONTAINED, object, parentId);
                }
            }
        }
        if (!(object instanceof Node)) {
            return;
        }
        final LongObjectMap<Node> children = ((Node) object).childrenMap();
        for (int i = 0; i < children.size(); i++) {
            final Node member = children.valueAt(i);
            if (view.matchedObjectIds().contains(member.id())) {
                view.enqueueMembership(ChangeKind.CONTAINED, member, object.id());
            }
        }
    }

    /**
     * A link names its ends by their ports, and the node a port is on is said by the port: a
     * view carrying a link carries the link's two ports too, and nothing further - the node at
     * the far end is known by id, not delivered.
     *
     * @param view view to ask about
     * @param port the port
     * @return whether a link the view delivers ends at it
     */
    private boolean carriesPort(final View view, final Port port) {
        final LongSet linkIds = linksAt(port);
        if (linkIds == null) {
            return false;
        }
        for (int i = 0; i < linkIds.size(); i++) {
            if (view.matchedObjectIds().contains(linkIds.valueAt(i))) {
                return true;
            }
        }
        return false;
    }

    private void refreshPortsOf(final View view, final Link link) {
        if (link.fromOutput() != null) {
            applyMembership(view, link.fromOutput());
        }
        if (link.toInput() != null) {
            applyMembership(view, link.toInput());
        }
    }

    private void refreshPortInAllViews(final Port port) {
        if (port == null) {
            return;
        }
        final View[] snapshot = views;
        for (int i = 0; i < snapshot.length; i++) {
            applyMembership(snapshot[i], port);
        }
    }

    private LongSet linksAt(final Port port) {
        return port.kind() == ObjectKind.INPUT
                ? linksByToInputId.get(port.id())
                : linksByFromOutputId.get(port.id());
    }

    private StructureObject requireObject(final long objectId) {
        final StructureObject candidate = objectsById.get(objectId);
        if (candidate == null) {
            throw new NoSuchElementException("No such object: " + objectId);
        }
        return candidate;
    }

    StructureObject requireObject(final long objectId,
                                          final ObjectKind expectedKind) {
        final StructureObject candidate = requireObject(objectId);
        if (candidate.kind() != expectedKind) {
            throw new IllegalArgumentException(
                    "Object '" + objectId + "' is " + candidate.kind()
                            + ", expected " + expectedKind);
        }
        return candidate;
    }

    private void indexLinkAdd(final Link link) {
        addToIndex(linksByToInputId, link.toInput().id(), link.id());
        addToIndex(linksByFromOutputId, link.fromOutput().id(), link.id());
        link.toInput().addLinks(1);
        link.fromOutput().addLinks(1);
    }

    private void indexLinkRemove(final Link link) {
        unindexLinkEnd(linksByToInputId, link.toInput(), link.id());
        unindexLinkEnd(linksByFromOutputId, link.fromOutput(), link.id());
    }

    private static void unindexLinkEnd(final LongObjectMap<LongSet> index,
                                       final Port port,
                                       final long linkId) {
        if (removeFromIndex(index, port.id(), linkId)) {
            port.addLinks(-1);
        }
    }

    /**
     * Tells the views what a link's coming or going did to the count at its ends, once the
     * link itself has been told, so a port never leaves a view before a link it carries.
     *
     * @param link the link
     */
    private void restateLinks(final Link link) {
        restateLinksOf(link.fromOutput());
        restateLinksOf(link.toInput());
    }

    private void restateLinksOf(final Port port) {
        if (port != null && objectsById.get(port.id()) == port) {
            markDirtyInAllViews(port, PropertyKeys.LINKS);
            recheck(port);
        }
    }

    private static void addToIndex(final LongObjectMap<LongSet> index,
                                   final long key, final long value) {
        LongSet set = index.get(key);
        if (set == null) {
            set = new LongSet();
            index.put(key, set);
        }
        set.add(value);
    }

    private static boolean removeFromIndex(final LongObjectMap<LongSet> index,
                                           final long key, final long value) {
        final LongSet set = index.get(key);
        if (set == null) {
            return false;
        }
        final boolean removed = set.remove(value);
        if (set.size() == 0) {
            index.remove(key);
        }
        return removed;
    }

    private void runPostCommitDrains() {
        final View[] snapshot = views;
        for (int i = 0; i < snapshot.length; i++) {
            if (snapshot[i].owes()) {
                snapshot[i].drain(false);
            }
        }
    }

    /**
     * Hands every view what it owes, now. Runs on the worker thread.
     */
    void drainPendingViews() {
        final View[] snapshot = views;
        for (int i = 0; i < snapshot.length; i++) {
            if (snapshot[i].owes()) {
                snapshot[i].drain(true);
            }
        }
    }

    /**
     * @param work what to run on the worker thread outside any task - a view's timer, or its
     *             wake-up once the delivery thread has made room
     * @return the work, run as this structure's own: a subscriber it calls that reaches the
     *         structure joins in rather than queueing behind it
     */
    Runnable asOwnTask(final Runnable work) {
        return new Runnable() {
            @Override
            public void run() {
                final Structure previous = CURRENT_EXECUTING_STRUCTURE.get();
                CURRENT_EXECUTING_STRUCTURE.set(Structure.this);
                try {
                    work.run();
                } finally {
                    CURRENT_EXECUTING_STRUCTURE.set(previous);
                }
            }
        };
    }

    /**
     * @param ownTask what {@link #asOwnTask(Runnable)} made; dropped if the structure is closing
     */
    void execute(final Runnable ownTask) {
        try {
            worker.execute(ownTask);
        } catch (final RejectedExecutionException ignored) {
        }
    }

    /**
     * @param ownTask    what {@link #asOwnTask(Runnable)} made
     * @param delayNanos how long from now
     * @return the timer
     */
    ScheduledFuture<?> after(final Runnable ownTask, final long delayNanos) {
        return scheduler.schedule(() -> execute(ownTask), delayNanos, TimeUnit.NANOSECONDS);
    }
}
