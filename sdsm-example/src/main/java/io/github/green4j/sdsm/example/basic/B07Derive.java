package io.github.green4j.sdsm.example.basic;

import io.github.green4j.sdsm.Fold;
import io.github.green4j.sdsm.Node;
import io.github.green4j.sdsm.Over;
import io.github.green4j.sdsm.Structure;
import io.github.green4j.sdsm.StructureRuntime;

import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;

/**
 * A group can say what its members come to: the backlog of a cluster is the sum of its
 * streams', the backlog of a region the sum of its clusters'. The structure keeps the sum as
 * members change, come and go, and says how many of the members it is made of - a sum over
 * members that have said nothing yet is not the whole of it.
 */
public final class B07Derive {

    public static void main(final String[] args) {
        run(System.out::println);
    }

    static void run(final Consumer<String> out) {
        try (StructureRuntime runtime = StructureRuntime.create(1, 1, "derive")) {
            final Structure structure = runtime.newStructure();
            final int backlog = structure.propertyKeys().idOf("backlog");
            final int lag = structure.propertyKeys().idOf("lag");

            final Node region = structure.submit(() -> structure.createNode("eu", "region")).join();
            final Node de1 = structure.submit(() -> structure.createNode("eu-de-1", "cluster")).join();
            final Node de2 = structure.submit(() -> structure.createNode("eu-de-2", "cluster")).join();
            final Node orders = stream(structure, de1, "orders");
            final Node trades = stream(structure, de1, "trades");
            final Node quotes = stream(structure, de2, "quotes");
            // A subgroup deriving the same key is a summand too, so the region sums clusters.
            structure.run(() -> {
                structure.contain(region.id(), de1.id(), "placement");
                structure.contain(region.id(), de2.id(), "placement");
                for (final Node group : new Node[]{region, de1, de2}) {
                    structure.derive(group.id(), backlog, Fold.SUM, Over.children(backlog, "stream"));
                    structure.derive(group.id(), lag, Fold.MAX, Over.children(lag, "stream"));
                }
            }).join();
            print(structure, out, region, de1, de2);

            out.accept("-- two streams say how far behind they are");
            structure.run(() -> {
                structure.setLong(orders.id(), backlog, 100L);
                structure.setLong(orders.id(), lag, 3L);
                structure.setLong(quotes.id(), backlog, 40L);
                structure.setLong(quotes.id(), lag, 9L);
            }).join();
            print(structure, out, region, de1, de2);

            out.accept("-- and the third");
            structure.run(() -> {
                structure.setLong(trades.id(), backlog, 5L);
                structure.setLong(trades.id(), lag, 1L);
            }).join();
            print(structure, out, region, de1, de2);

            out.accept("-- quotes is removed");
            structure.run(() -> structure.remove(quotes.id())).join();
            print(structure, out, region, de1, de2);

            // What the structure works out is not written by anyone else.
            out.accept("-- a caller writes the backlog of a cluster");
            try {
                structure.run(() -> structure.setLong(de1.id(), backlog, 0L)).join();
            } catch (final CompletionException e) {
                out.accept("refused: " + e.getCause().getMessage());
            }
        }
    }

    private static Node stream(final Structure structure, final Node cluster,
                               final String name) {
        final Node stream = structure.submit(() -> structure.createNode(name, "stream")).join();
        structure.run(() -> structure.contain(cluster.id(), stream.id(), "placement")).join();
        return stream;
    }

    private static void print(final Structure structure, final Consumer<String> out,
                              final Node... groups) {
        for (final Node group : groups) {
            final Map<String, Object> held = structure.snapshotObject(group.id()).join();
            out.accept("  " + held.get("name")
                    + " backlog=" + held.getOrDefault("backlog", "-")
                    + " (" + held.get("backlog.known") + " of " + held.get("backlog.total") + ")"
                    + " lag=" + held.getOrDefault("lag", "-"));
        }
    }

    private B07Derive() {
    }
}
