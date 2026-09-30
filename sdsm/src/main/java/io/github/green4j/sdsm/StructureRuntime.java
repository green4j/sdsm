package io.github.green4j.sdsm;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The threads structures run on: each structure is bound to one worker, and a worker may serve
 * several. Closing the runtime closes every structure it made.
 */
public final class StructureRuntime implements AutoCloseable {

    private static final long SHUTDOWN_TIMEOUT_SECONDS = 5;

    private final ExecutorService[] workers;
    private final ScheduledExecutorService scheduler;
    private final AtomicInteger roundRobin = new AtomicInteger(0);

    private final List<Structure> createdStructures = new ArrayList<>();
    private volatile boolean closed = false;

    /**
     * @param workerCount      number of single-thread worker executors
     * @param schedulerThreads number of threads in the scheduler pool
     * @param namePrefix       prefix for thread names
     * @return the new runtime
     */
    public static StructureRuntime create(final int workerCount,
                                          final int schedulerThreads,
                                          final String namePrefix) {
        if (workerCount <= 0) {
            throw new IllegalArgumentException("workerCount must be > 0");
        }
        if (schedulerThreads <= 0) {
            throw new IllegalArgumentException("schedulerThreads must be > 0");
        }

        final ExecutorService[] workers = new ExecutorService[workerCount];
        for (int i = 0; i < workerCount; i++) {
            final int index = i;
            workers[i] = Executors.newSingleThreadExecutor(r -> {
                final Thread t = new Thread(r, namePrefix + "-worker-" + index);
                t.setDaemon(true);
                return t;
            });
        }

        final ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(
                schedulerThreads, r -> {
            final Thread t = new Thread(r, namePrefix + "-scheduler");
            t.setDaemon(true);
            return t;
        });
        scheduler.setRemoveOnCancelPolicy(true);
        scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);

        return new StructureRuntime(workers, scheduler);
    }

    private StructureRuntime(final ExecutorService[] workers,
                             final ScheduledExecutorService scheduler) {
        this.workers = workers;
        this.scheduler = scheduler;
    }

    /**
     * Creates a new Structure on the next worker in turn.
     *
     * @return the new structure
     */
    public Structure newStructure() {
        synchronized (createdStructures) {
            if (closed) {
                throw new IllegalStateException("StructureRuntime is closed");
            }

            final int workerIndex = Math.floorMod(roundRobin.getAndIncrement(), workers.length);
            final ExecutorService assignedWorker = workers[workerIndex];

            final Structure structure = new Structure(assignedWorker, scheduler);
            structure.setOnCloseListener(() -> {
                synchronized (createdStructures) {
                    createdStructures.remove(structure);
                }
            });

            createdStructures.add(structure);
            return structure;
        }
    }

    /**
     * Closes every structure, then the threads; a task still waiting when they stop fails.
     */
    @Override
    public void close() {
        final List<Structure> snapshot;
        synchronized (createdStructures) {
            if (closed) {
                return;
            }
            closed = true;
            snapshot = new ArrayList<>(createdStructures);
            createdStructures.clear();
        }

        for (final Structure structure : snapshot) {
            structure.close();
        }

        for (final ExecutorService worker : workers) {
            worker.shutdown();
        }
        scheduler.shutdown();

        try {
            for (final ExecutorService worker : workers) {
                if (!worker.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    worker.shutdownNow();
                }
            }
            if (!scheduler.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (final InterruptedException interrupted) {
            for (final ExecutorService worker : workers) {
                worker.shutdownNow();
            }
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // a task admitted after its structure cleared its futures, and never run
        final RejectedExecutionException cause =
                new RejectedExecutionException("StructureRuntime has been closed");
        for (final Structure structure : snapshot) {
            structure.completeAllOutstanding(cause);
        }
    }

    /**
     * @return whether it has been closed
     */
    public boolean isClosed() {
        return closed;
    }

    /**
     * @return how many structures are not closed yet
     */
    int registeredStructureCount() {
        synchronized (createdStructures) {
            return createdStructures.size();
        }
    }
}