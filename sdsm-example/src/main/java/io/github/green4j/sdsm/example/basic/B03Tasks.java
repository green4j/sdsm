package io.github.green4j.sdsm.example.basic;

import io.github.green4j.sdsm.DeliveryPolicy;
import io.github.green4j.sdsm.Node;
import io.github.green4j.sdsm.Structure;
import io.github.green4j.sdsm.StructureRuntime;
import io.github.green4j.sdsm.View;

import java.time.Duration;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * A task is what runs on the structure's thread, and everything one task changes reaches a
 * view as one batch: a subscriber never sees half of it. A task can name who is writing, and
 * a view can be told to hold its deliveries apart, so a burst of changes arrives as one.
 */
public final class B03Tasks {

    private static final int DEPLOYER = 1;
    private static final int METRICS = 2;

    public static void main(final String[] args) {
        run(System.out::println);
    }

    static void run(final Consumer<String> out) {
        try (StructureRuntime runtime = StructureRuntime.create(1, 1, "tasks")) {
            final Structure structure = runtime.newStructure();
            final int status = structure.propertyKeys().idOf("status");
            final int cpu = structure.propertyKeys().idOf("cpu");

            final View nodes = structure.createView("nodes", "node", null,
                    DeliveryPolicy.onChange()).join();
            final AtomicInteger delivered = new AtomicInteger();
            structure.subscribe(nodes.id(), batch -> {
                Show.batch("nodes", batch, out);
                delivered.incrementAndGet();
            }).join();

            // Every task is a batch of its own.
            out.accept("-- three tasks");
            final Node a = structure.submit(() -> structure.createNode("a", "service")).join();
            final Node b = structure.submit(() -> structure.createNode("b", "service")).join();
            structure.run(() -> structure.setText(a.id(), status, "STARTING")).join();

            // A call made inside a task runs there and then, and is part of it.
            out.accept("-- one task");
            structure.run(() -> {
                final Node c = structure.createNode("c", "service");
                structure.setText(c.id(), status, "STARTING");
                structure.setText(a.id(), status, "READY");
            }).join();

            // The id is the caller's own number; the structure keeps it with every property
            // the task writes, so two writers of one object can each tell what is theirs.
            out.accept("-- two sources write one node");
            structure.run(DEPLOYER, () -> structure.setText(b.id(), status, "READY")).join();
            structure.run(METRICS, () -> structure.setLong(b.id(), cpu, 40L)).join();
            out.accept("status is written by " + structure.submit(() -> b.sourceOf(status)).join()
                    + ", cpu by " + structure.submit(() -> b.sourceOf(cpu)).join());

            // A failed task is not rolled back: what it did before failing is delivered.
            out.accept("-- a task fails");
            try {
                structure.run(() -> {
                    structure.setText(b.id(), status, "FAILED");
                    throw new IllegalStateException("the task broke");
                }).join();
            } catch (final CompletionException e) {
                out.accept("the task failed: " + e.getCause().getMessage());
            }

            // Held a second apart, three writes arrive as one, carrying the value last written.
            out.accept("-- three writes, held apart");
            structure.setViewDeliveryPolicy(nodes.id(),
                    DeliveryPolicy.minInterval(Duration.ofSeconds(1))).join();
            final int before = delivered.get();
            for (long load = 41L; load <= 43L; load++) {
                final long value = load;
                structure.run(() -> structure.setLong(b.id(), cpu, value)).join();
            }
            Show.await(() -> delivered.get() > before);

            // Whoever has fallen behind asks for the whole view again, for itself alone.
            out.accept("-- one subscriber asks for the whole view");
            structure.snapshotView(nodes.id(), batch -> Show.batch("again", batch, out)).join();
        }
    }

    private B03Tasks() {
    }
}
