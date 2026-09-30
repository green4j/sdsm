package io.github.green4j.sdsm;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SDSM runs no delivery threads of its own. Without an executor the structure's own thread calls
 * the subscribers; given one, that is where they run, and the model stops waiting on them.
 */
class DeliveryTest {
    private static final long TIMEOUT_SECONDS = 5L;

    private final StructureRuntime runtime = StructureRuntime.create(1, 1, "delivery");
    private final Structure structure = runtime.newStructure();
    private final ExecutorService deliveryThread = Executors.newSingleThreadExecutor(
            r -> new Thread(r, "delivery-thread"));

    @AfterEach
    void tearDown() {
        runtime.close();
        deliveryThread.shutdownNow();
    }

    private View allNodes() {
        return structure.createView("nodes", "node", null,
                DeliveryPolicy.onChange()).join();
    }

    @Test
    void shouldCallSubscribersOnTheGivenExecutor() throws InterruptedException {
        structure.withDeliveryExecutor(deliveryThread);
        final CountDownLatch delivered = new CountDownLatch(1);
        final String[] calledOn = new String[1];

        structure.subscribe(allNodes().id(), batch -> {
            calledOn[0] = Thread.currentThread().getName();
            delivered.countDown();
        }).join();
        structure.submit(() -> structure.createNode("Alpha", "pod")).join();

        assertTrue(delivered.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "nothing was delivered");
        assertEquals("delivery-thread", calledOn[0]);
    }

    /**
     * A delivery executor that runs only when told, so a test can hold delivery back as long as
     * it likes.
     */
    private static final class HeldBack implements Executor {
        private final Queue<Runnable> queued = new ConcurrentLinkedQueue<>();

        @Override
        public void execute(final Runnable task) {
            queued.add(task);
        }

        void release() {
            for (Runnable task = queued.poll(); task != null; task = queued.poll()) {
                task.run();
            }
        }
    }

    private void write(final Node node, final int key, final int times) {
        for (int i = 0; i < times; i++) {
            final long value = i;
            structure.run(() -> structure.setLong(node.id(), key, value)).join();
        }
    }

    /**
     * Keeps releasing what has been handed to the executor until the subscriber has seen the
     * value, or a second has gone: what is owed may reach the executor from the structure's thread
     * at any moment.
     *
     * @param delivery the executor held back
     * @param recorder the subscriber
     * @param objectId the object written
     * @param value    the value last written to its key {@code k}
     * @throws InterruptedException if interrupted while waiting
     */
    private static void releaseUntil(final HeldBack delivery,
                                     final Recorder recorder,
                                     final long objectId,
                                     final String value) throws InterruptedException {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1L);
        while (!value.equals(recorder.latestValueOf(objectId, "k"))
                && System.nanoTime() < deadline) {
            delivery.release();
            Thread.sleep(1L);
        }
    }

    @Test
    void shouldKeepWhatAnExecutorFallingBehindCannotTakeYet() throws InterruptedException {
        final HeldBack delivery = new HeldBack();
        structure.withDeliveryExecutor(delivery);
        final int key = structure.propertyKeys().idOf("k");
        final Node alpha = structure.submit(() -> structure.createNode("Alpha", "pod")).join();
        final View view = allNodes();
        final Recorder recorder = new Recorder();
        structure.subscribe(view.id(), recorder).join();

        write(alpha, key, 100);
        structure.flushAll().join();
        structure.snapshotView(view.id(), recorder).join();
        releaseUntil(delivery, recorder, alpha.id(), "99");

        assertEquals("99", recorder.latestValueOf(alpha.id(), "k"),
                "the last value arrives once there is room, with nothing written after it");
        assertNull(recorder.failure());
    }

    @Test
    void shouldSendNothingToASubscriberThatHasLeft() {
        final HeldBack delivery = new HeldBack();
        structure.withDeliveryExecutor(delivery);
        final View view = allNodes();
        final Recorder leaving = new Recorder();
        structure.subscribe(view.id(), leaving).join().run();
        structure.flushAll().join();

        delivery.release();

        assertEquals(0, leaving.batches(), "its snapshot was on the way when it left");
    }

    @Test
    void shouldSendNothingOnceItsViewHasGone() {
        final HeldBack delivery = new HeldBack();
        structure.withDeliveryExecutor(delivery);
        final View view = allNodes();
        final Recorder recorder = new Recorder();
        structure.subscribe(view.id(), recorder).join();
        structure.removeView(view.id()).join();

        delivery.release();

        assertEquals(0, recorder.batches(), "nothing after being told the view has gone");
        assertTrue(recorder.failure() instanceof IllegalStateException, String.valueOf(recorder.failure()));
    }

    @Test
    void shouldGiveALateSubscriberItsSnapshotBeforeAnyDelta() {
        final HeldBack delivery = new HeldBack();
        structure.withDeliveryExecutor(delivery);
        final View view = allNodes();
        final Recorder early = new Recorder();
        final Recorder late = new Recorder();
        structure.subscribe(view.id(), early).join();
        structure.submit(() -> structure.createNode("Alpha", "pod")).join();
        structure.subscribe(view.id(), late).join();
        structure.submit(() -> structure.createNode("Beta", "pod")).join();

        delivery.release();

        assertEquals(List.of("S0", "D1", "D2"), early.sequence());
        assertEquals(List.of("S1", "D2"), late.sequence(),
                "a snapshot states everything up to the delta it is numbered as");
    }

    @Test
    void shouldRestateAViewToOneSubscriberWithoutAGapForAnother() {
        final View view = allNodes();
        final Recorder asking = new Recorder();
        final Recorder other = new Recorder();
        structure.subscribe(view.id(), asking).join();
        structure.subscribe(view.id(), other).join();

        final Node alpha = structure.submit(() -> structure.createNode("Alpha", "pod")).join();
        structure.snapshotView(view.id(), asking).join();
        structure.submit(() -> structure.createNode("Beta", "pod")).join();

        assertEquals(List.of("S0", "D1", "S1", "D2"), asking.sequence());
        assertEquals(List.of(alpha.id()), asking.idsInSnapshot(ChangeKind.ADDED),
                "the view restated as it is now");
        assertEquals(List.of("S0", "D1", "D2"), other.sequence(),
                "a snapshot for one subscriber is no gap for another");
    }

    @Test
    void shouldFinishABatchBeforeTheOneASubscriberWrote() {
        final int key = structure.propertyKeys().idOf("k");
        final Node alpha = structure.submit(() -> structure.createNode("Alpha", "pod")).join();
        final View view = allNodes();
        final BatchSubscriber writing = batch -> {
            if (!batch.isInitialSnapshot() && batch.batchSequenceNumber() == 1L) {
                structure.run(() -> structure.setLong(alpha.id(), key, 2L));
            }
        };
        final Recorder other = new Recorder();
        structure.subscribe(view.id(), writing).join();
        structure.subscribe(view.id(), other).join();

        structure.run(() -> structure.setLong(alpha.id(), key, 1L)).join();

        assertEquals(List.of("S0", "D1", "D2"), other.sequence());
    }

    /**
     * A subscriber called on the structure's thread is not inside a task: a write it makes
     * straight away would reach nobody until some later task ended, so it is refused.
     */
    @Test
    void shouldRefuseAWriteOutsideATask() {
        final int key = structure.propertyKeys().idOf("k");
        final Node alpha = structure.submit(() -> structure.createNode("Alpha", "pod")).join();
        final View view = allNodes();
        final Queue<Throwable> refused = new ConcurrentLinkedQueue<>();
        final BatchSubscriber writing = new BatchSubscriber() {
            @Override
            public void onBatch(final StructureBatch batch) {
                if (!batch.isInitialSnapshot()) {
                    structure.setLong(alpha.id(), key, 2L);
                }
            }

            @Override
            public void onError(final Throwable failure) {
                refused.add(failure);
            }
        };
        structure.subscribe(view.id(), writing).join();

        structure.run(() -> structure.setLong(alpha.id(), key, 1L)).join();

        assertTrue(refused.peek() instanceof IllegalStateException, "the write was taken: " + refused);
        assertEquals(1L, structure.snapshotObject(alpha.id()).join().get("k"));
    }

    @Test
    void shouldHandOverWhatIsOwedBeforeARestatement() {
        final View view = structure.createView("nodes", "node", null,
                DeliveryPolicy.minInterval(Duration.ofMinutes(1L))).join();
        final Recorder recorder = new Recorder();
        structure.subscribe(view.id(), recorder).join();
        structure.submit(() -> structure.createNode("Alpha", "pod")).join();

        structure.snapshotView(view.id(), recorder).join();
        structure.flushAll().join();

        assertEquals(List.of("S0", "D1", "S1"), recorder.sequence(),
                "nothing after the snapshot repeats what it said");
    }

    @Test
    void shouldKeepWhatIsOwedWhenASelectorIsRefused() {
        final int key = structure.propertyKeys().idOf("k");
        final Node alpha = structure.submit(() -> structure.createNode("Alpha", "pod")).join();
        final View view = structure.createView("nodes", "node", null,
                DeliveryPolicy.minInterval(Duration.ofMinutes(1L))).join();
        final Recorder recorder = new Recorder();
        structure.subscribe(view.id(), recorder).join();
        structure.run(() -> structure.setLong(alpha.id(), key, 7L)).join();

        assertThrows(CompletionException.class,
                () -> structure.setViewSelector(view.id(), "node[").join());
        structure.flushAll().join();

        assertEquals("7", recorder.latestValueOf(alpha.id(), "k"));
    }
}
