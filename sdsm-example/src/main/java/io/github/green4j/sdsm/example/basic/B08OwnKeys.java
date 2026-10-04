package io.github.green4j.sdsm.example.basic;

import io.github.green4j.sdsm.Fold;
import io.github.green4j.sdsm.Node;
import io.github.green4j.sdsm.Over;
import io.github.green4j.sdsm.Structure;
import io.github.green4j.sdsm.StructureRuntime;

import java.util.Map;
import java.util.function.Consumer;

/**
 * Two sources measure one thing: the broker says how much a stream holds, the exporter says
 * it too, and they do not always agree or always answer. Each writes a property of its own,
 * and the structure folds them into the one a display reads - which a parent can then fold
 * like any other.
 */
public final class B08OwnKeys {

    private static final int BROKER = 1;
    private static final int EXPORTER = 2;

    public static void main(final String[] args) {
        run(System.out::println);
    }

    static void run(final Consumer<String> out) {
        try (StructureRuntime runtime = StructureRuntime.create(1, 1, "own-keys")) {
            final Structure structure = runtime.newStructure();
            final int backlog = structure.propertyKeys().idOf("backlog");
            final int byBroker = structure.propertyKeys().idOf("backlog.broker");
            final int byExporter = structure.propertyKeys().idOf("backlog.exporter");

            final Node cluster = structure.submit(() -> structure.createNode("eu-de-1", "cluster")).join();
            final Node orders = structure.submit(() -> structure.createNode("orders", "stream")).join();
            final Node shipments = structure.submit(() -> structure.createNode("shipments", "stream")).join();
            // The worse of the two answers stands for the stream, and the cluster sums them.
            structure.run(() -> {
                for (final Node stream : new Node[]{orders, shipments}) {
                    structure.contain(cluster.id(), stream.id(), "placement");
                    structure.derive(stream.id(), backlog, Fold.MAX, Over.keys(byBroker, byExporter));
                }
                structure.derive(cluster.id(), backlog, Fold.SUM, Over.children(backlog, "stream"));
            }).join();
            print(structure, out, orders, shipments, cluster);

            out.accept("-- the broker answers");
            structure.run(BROKER, () -> {
                structure.setLong(orders.id(), byBroker, 100L);
                structure.setLong(shipments.id(), byBroker, 20L);
            }).join();
            print(structure, out, orders, shipments, cluster);

            out.accept("-- the exporter answers, and sees more on orders");
            structure.run(EXPORTER, () -> {
                structure.setLong(orders.id(), byExporter, 130L);
                structure.setLong(shipments.id(), byExporter, 10L);
            }).join();
            print(structure, out, orders, shipments, cluster);

            out.accept("-- the exporter no longer answers on orders");
            structure.run(EXPORTER, () -> structure.removeProperty(orders.id(), byExporter))
                    .join();
            print(structure, out, orders, shipments, cluster);
        }
    }

    private static void print(final Structure structure, final Consumer<String> out,
                              final Node orders, final Node shipments, final Node cluster) {
        for (final Node stream : new Node[]{orders, shipments}) {
            final Map<String, Object> held = structure.snapshotObject(stream.id()).join();
            out.accept("  " + held.get("$name")
                    + " broker=" + held.getOrDefault("backlog.broker", "-")
                    + " exporter=" + held.getOrDefault("backlog.exporter", "-")
                    + " backlog=" + held.getOrDefault("backlog", "-"));
        }
        out.accept("  " + cluster.name() + " backlog=" + structure.snapshotObject(cluster.id())
                .join().getOrDefault("backlog", "-"));
    }

    private B08OwnKeys() {
    }
}
