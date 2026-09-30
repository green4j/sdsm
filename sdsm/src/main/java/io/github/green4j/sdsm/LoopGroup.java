package io.github.green4j.sdsm;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The loops of one structure and the threads they run on. Loops are what keeps one cluster's
 * ingest off another's thread; how many threads there are is how far that separation goes.
 */
public final class LoopGroup implements AutoCloseable {

    private static final long SHUTDOWN_TIMEOUT_SECONDS = 5;

    private final Structure structure;
    private final ExecutorService[] threads;
    private final LoopThread[] loopThreads;
    private final List<EventLoop> loops = new ArrayList<>();
    private final AtomicInteger roundRobin = new AtomicInteger(0);
    private volatile boolean closed;

    /**
     * @param structure   what the loops of this group assemble
     * @param threadCount how many threads the loops are spread over
     * @param namePrefix  prefix for thread names
     * @return the new group
     */
    public static LoopGroup create(final Structure structure,
                                   final int threadCount,
                                   final String namePrefix) {
        if (structure == null) {
            throw new IllegalArgumentException("structure is required");
        }
        if (threadCount <= 0) {
            throw new IllegalArgumentException("threadCount must be > 0");
        }
        final ExecutorService[] threads = new ExecutorService[threadCount];
        for (int i = 0; i < threadCount; i++) {
            final int index = i;
            threads[i] = Executors.newSingleThreadExecutor(runnable -> {
                final Thread created = new Thread(runnable, namePrefix + "-loop-" + index);
                created.setDaemon(true);
                return created;
            });
        }
        return new LoopGroup(structure, threads);
    }

    private LoopGroup(final Structure structure, final ExecutorService[] threads) {
        this.structure = structure;
        this.threads = threads;
        this.loopThreads = new LoopThread[threads.length];
        for (int i = 0; i < threads.length; i++) {
            loopThreads[i] = new LoopThread(threads[i], structure);
        }
    }

    /**
     * @return a loop of its own, on the next thread in turn
     */
    public EventLoop newLoop() {
        synchronized (loops) {
            if (closed) {
                throw new IllegalStateException("LoopGroup is closed");
            }
            final int index = Math.floorMod(roundRobin.getAndIncrement(), threads.length);
            final EventLoop loop = new EventLoop(this, loopThreads[index]);
            loops.add(loop);
            return loop;
        }
    }

    @Override
    public void close() {
        final EventLoop[] snapshot;
        synchronized (loops) {
            if (closed) {
                return;
            }
            closed = true;
            snapshot = loops.toArray(new EventLoop[0]);
            loops.clear();
        }
        for (int i = 0; i < snapshot.length; i++) {
            snapshot[i].close();
        }
        for (int i = 0; i < threads.length; i++) {
            threads[i].shutdown();
        }
        try {
            for (int i = 0; i < threads.length; i++) {
                if (!threads[i].awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    threads[i].shutdownNow();
                }
            }
        } catch (final InterruptedException interrupted) {
            for (int i = 0; i < threads.length; i++) {
                threads[i].shutdownNow();
            }
            Thread.currentThread().interrupt();
        }
    }

    boolean isClosed() {
        return closed;
    }

    Structure structure() {
        return structure;
    }
}
