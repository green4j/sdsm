package io.github.green4j.sdsm;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

/**
 * One thread of a group and what its feeds have ready for the structure. The batches of every
 * feed on the thread go over as one task: a round costs one handover per thread rather than one
 * per feed. Each batch is still applied under its own source, so provenance stays exact; what
 * is lost is only that a subscriber sees the round as one batch.
 * <p>
 * Everything but {@link #execute(Runnable)} runs on the thread itself, apart from the apply,
 * which runs on the structure's and is ordered by the handover.
 */
final class LoopThread {

    private final ExecutorService thread;
    private final Structure.Handover handover;
    private final Runnable handTask = this::handOver;
    private final Runnable appliedTask = this::applied;

    private Feed<?>[] ready = new Feed<?>[8];
    private int readyCount;
    private Feed<?>[] handed = new Feed<?>[8];
    private int handedCount;
    private boolean inFlight;
    private boolean handScheduled;
    private Throwable handoverFailure;

    LoopThread(final ExecutorService thread, final Structure structure) {
        this.thread = thread;
        this.handover = structure.handover(Structure.NO_SOURCE, this::applyOnStructure,
                this::afterApply);
    }

    void execute(final Runnable task) {
        try {
            thread.execute(task);
        } catch (final RejectedExecutionException shuttingDown) {
            return;                     // the group is closing, and so is this thread
        }
    }

    /**
     * A feed has a batch for the structure. It goes over once the drains already queued on
     * this thread have had their say, so that they go with it.
     *
     * @param feed the feed
     */
    void ready(final Feed<?> feed) {
        if (readyCount == ready.length) {
            ready = Arrays.copyOf(ready, readyCount * 2);
        }
        ready[readyCount++] = feed;
        if (!inFlight && !handScheduled) {
            handScheduled = true;
            execute(handTask);
        }
    }

    private void handOver() {
        handScheduled = false;
        if (inFlight || readyCount == 0) {
            return;                     // what is in flight will come back for the rest
        }
        final Feed<?>[] swap = handed;
        handed = ready;
        handedCount = readyCount;
        ready = swap;
        readyCount = 0;
        inFlight = true;
        handover.ingest();
    }

    private void applyOnStructure() {
        for (int i = 0; i < handedCount; i++) {
            handed[i].applyOnStructure();
        }
    }

    private void afterApply(final Throwable failure) {
        handoverFailure = failure;
        execute(appliedTask);
    }

    private void applied() {
        final Throwable failure = handoverFailure;
        handoverFailure = null;
        inFlight = false;
        RuntimeException first = null;
        for (int i = 0; i < handedCount; i++) {
            try {
                handed[i].applied(failure);
            } catch (final RuntimeException thrown) {
                if (first == null) {
                    first = thrown;
                } else {
                    first.addSuppressed(thrown);
                }
            }
            handed[i] = null;
        }
        handedCount = 0;
        if (readyCount > 0 && !handScheduled) {
            handScheduled = true;       // what got ready while this was in flight
            execute(handTask);
        }
        if (first != null) {
            throw first;                // after every feed has had its batch back
        }
    }
}
