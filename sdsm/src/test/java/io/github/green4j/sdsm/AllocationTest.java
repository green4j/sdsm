package io.github.green4j.sdsm;

import com.sun.management.ThreadMXBean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.function.IntConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A steady stream of changes must cost the structure's thread nothing to carry: the key is an
 * int, the value is typed, the batch is a buffer that comes back empty. What is measured is the
 * slope - how many more bytes ten times the changes cost - so the fixed cost of standing the
 * measurement up does not enter into it.
 */
class AllocationTest {
    private static final int NODES = 64;
    private static final int KEYS = 4;
    private static final int DRAIN_EVERY = 16;

    private static final int FEW = 2_000;
    private static final int MANY = 20_000;
    private static final int PASSES = 3;

    private final StructureRuntime runtime = StructureRuntime.create(1, 1, "alloc");
    private final Structure structure = runtime.newStructure();
    private final Counting subscriber = new Counting();

    private final long[] nodeIds = new long[NODES];
    private final int[] keyIds = new int[KEYS];

    private ThreadMXBean threads;
    private long missed;

    @BeforeEach
    void setUp() {
        final java.lang.management.ThreadMXBean plain = ManagementFactory.getThreadMXBean();
        Assumptions.assumeTrue(plain instanceof ThreadMXBean, "no per-thread allocation counter");
        threads = (ThreadMXBean) plain;
        Assumptions.assumeTrue(threads.isThreadAllocatedMemorySupported());
        threads.setThreadAllocatedMemoryEnabled(true);

        structure.run(() -> {
            final int state = structure.propertyKeys().idOf("state");
            for (int i = 0; i < NODES; i++) {
                nodeIds[i] = structure.createNode("pod-" + i, "pod", "k8s:pod:" + i).id();
                structure.setText(nodeIds[i], state, "running");
            }
        }).join();
        for (int i = 0; i < KEYS; i++) {
            keyIds[i] = structure.propertyKeys().idOf("metric-" + i);
        }
        final View view = structure.createView("running", "node[state=running]", null,
                DeliveryPolicy.onChange()).join();
        structure.subscribe(view.id(), subscriber).join();
    }

    @AfterEach
    void tearDown() {
        runtime.close();
    }

    @Test
    void shouldCarryAStreamOfChangesWithoutAllocating() {
        final long growth = growthOf(i -> {
            structure.setLong(nodeIds[i % NODES], keyIds[i % KEYS], i);
            drainEvery(i);
        });

        assertTrue(subscriber.records > 0, "nothing was delivered");
        assertEquals(0L, growth, (MANY - FEW) + " further changes allocated " + growth + " bytes");
    }

    /**
     * Folds carry the stream at no cost of their own: the largest of a node's children, and the
     * sum over the members of a group.
     */
    @Test
    void shouldFoldAStreamOfChangesWithoutAllocating() {
        structure.run(() -> {
            final long cluster = structure.createNode("cluster", "cluster").id();
            for (int i = 0; i < NODES; i++) {
                structure.contain(cluster, nodeIds[i], "placement");
            }
            structure.derive(cluster, keyIds[0], Fold.MAX, Over.children(keyIds[0], "pod"));
            final Grouping states = structure.groupBy("states", "state", "node[type=pod]", "state");
            structure.deriveEach(states.id(), keyIds[1], Fold.SUM, Over.members("metric-1"));
        }).join();
        shouldCarryAStreamOfChangesWithoutAllocating();
    }

    /**
     * A member moving between groups that are there already costs nothing: its key is looked up
     * as it is spelled, not made into a string.
     */
    @Test
    void shouldMoveMembersBetweenGroupsWithoutAllocating() {
        final int phase = structure.propertyKeys().idOf("phase");
        structure.run(() -> {
            for (int i = 0; i < NODES; i++) {
                structure.setLong(nodeIds[i], phase, i % 2);
            }
            structure.groupBy("phases", "phase-group", "node[type=pod]", "phase");
        }).join();

        final long growth = growthOf(i -> {
            final int node = i % NODES;
            structure.setLong(nodeIds[node], phase, (i / NODES + node + 1) % 2);
            drainEvery(i);
        });

        assertEquals(0L, growth, (MANY - FEW) + " further moves allocated " + growth + " bytes");
    }

    /**
     * A binding resolves an external id on every observation it takes, so the resolution has to
     * cost nothing: the id is read as characters, and one StringBuilder serves for every lookup.
     */
    @Test
    void shouldResolveAnExternalIdWithoutAllocating() {
        final StringBuilder probe = new StringBuilder(32);

        final long growth = growthOf(i -> {
            probe.setLength(0);
            probe.append("k8s:pod:").append(i % NODES);
            if (structure.findByExternalId(probe) == null) {
                missed++;
            }
        });

        assertEquals(0L, missed, "lookups missed");
        assertEquals(0L, growth, (MANY - FEW) + " further lookups allocated " + growth + " bytes");
    }

    private void drainEvery(final int i) {
        if (i % DRAIN_EVERY == 0) {
            structure.drainPendingViews();
        }
    }

    /**
     * @param step one unit of the work, given its number
     * @return how many more bytes {@code MANY} units took than {@code FEW}, once warmed up
     */
    private long growthOf(final IntConsumer step) {
        for (int warmup = 0; warmup < 8; warmup++) {
            bytesFor(MANY, step);
        }
        return leastBytesFor(MANY, step) - leastBytesFor(FEW, step);
    }

    /**
     * A pass can only be made dearer by what surrounds it - a recompilation on the measured
     * thread, a counter read - so the least of several passes is what the work itself costs.
     *
     * @param units how many units of the work to do
     * @param step  one unit of the work
     * @return the fewest bytes a pass of them took
     */
    private long leastBytesFor(final int units, final IntConsumer step) {
        long least = Long.MAX_VALUE;
        for (int pass = 0; pass < PASSES; pass++) {
            least = Math.min(least, bytesFor(units, step));
        }
        return least;
    }

    @SuppressWarnings("deprecation")
    private long bytesFor(final int units, final IntConsumer step) {
        final long[] measured = new long[1];
        structure.run(() -> {
            final long self = Thread.currentThread().getId();
            final long before = threads.getThreadAllocatedBytes(self);
            for (int i = 0; i < units; i++) {
                step.accept(i);
            }
            structure.drainPendingViews();
            measured[0] = threads.getThreadAllocatedBytes(self) - before;
        }).join();
        return measured[0];
    }

    private static final class Counting implements BatchSubscriber {
        private long records;
        private long sum;

        @Override
        public void onBatch(final StructureBatch batch) {
            final ChangeCursor cursor = batch.cursor();
            while (cursor.next()) {
                records++;
                if (cursor.valueType() == ValueType.LONG) {
                    sum += cursor.longValue();
                }
            }
        }
    }
}
