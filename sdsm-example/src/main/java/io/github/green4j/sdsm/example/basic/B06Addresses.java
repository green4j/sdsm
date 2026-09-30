package io.github.green4j.sdsm.example.basic;

import io.github.green4j.sdsm.Structure;
import io.github.green4j.sdsm.StructureRuntime;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;

/**
 * Links nobody draws by hand. A port says it provides an address or requires one, and the
 * structure draws a link wherever the two meet, in whichever order they were said - which is
 * what lets two sources that see the two ends apart build one graph.
 * <p>
 * Blue and green run the same pipeline under the same addresses. A domain tells the copies
 * apart: a port reaches its own domain and what belongs to none, and another domain only
 * where a route is open.
 */
public final class B06Addresses {

    public static void main(final String[] args) {
        run(System.out::println);
    }

    static void run(final Consumer<String> out) {
        try (StructureRuntime runtime = StructureRuntime.create(1, 1, "addresses")) {
            final Structure structure = runtime.newStructure();

            final long writer = output(structure, "writer");
            final long reader = input(structure, "reader", "in");

            out.accept("-- the reader looks for orders; nobody provides them yet");
            structure.run(() -> structure.require(reader, "orders")).join();
            links(structure, out);

            out.accept("-- the writer provides orders");
            structure.run(() -> structure.provide(writer, "orders")).join();
            links(structure, out);

            // The link is the declarations': it goes when one of them does, and not by hand.
            out.accept("-- the link cannot be removed, only the declaration");
            final long drawn = structure.matchedObjectIds("link").join()[0];
            try {
                structure.run(() -> structure.remove(drawn)).join();
            } catch (final CompletionException e) {
                out.accept("refused: " + e.getCause().getMessage());
            }
            structure.run(() -> structure.clearAddress(writer)).join();
            links(structure, out);

            out.accept("-- blue and green, each with a writer and a reader of orders,"
                    + " and one market both read prices from");
            final int blue = structure.domains().idOf("blue");
            final int green = structure.domains().idOf("green");
            final long blueWriter = output(structure, "blue-writer");
            final long greenWriter = output(structure, "green-writer");
            final long market = output(structure, "market");
            final long blueOrders = input(structure, "blue-reader", "orders");
            final long bluePrices = input(structure, "blue-reader", "prices");
            final long greenOrders = input(structure, "green-reader", "orders");
            final long greenPrices = input(structure, "green-reader", "prices");
            structure.run(() -> {
                structure.provide(blueWriter, "orders", blue);
                structure.provide(greenWriter, "orders", green);
                structure.provide(market, "prices");
                structure.require(blueOrders, "orders", blue);
                structure.require(bluePrices, "prices", blue);
                structure.require(greenOrders, "orders", green);
                structure.require(greenPrices, "prices", green);
            }).join();
            links(structure, out);

            out.accept("-- green may read from blue");
            structure.run(() -> structure.allowRoute(green, blue)).join();
            links(structure, out);

            out.accept("-- and no longer");
            structure.run(() -> structure.denyRoute(green, blue)).join();
            links(structure, out);
        }
    }

    private static long output(final Structure structure, final String service) {
        final long node = structure.submit(() -> structure.createNode(service, "service")).join().id();
        return structure.submit(() -> structure.addOutput(node, "out", "tcp")).join().id();
    }

    private static long input(final Structure structure, final String service, final String name) {
        final long[] found = structure.matchedObjectIds("node[name=" + service + "]").join();
        final long node = found.length > 0
                ? found[0]
                : structure.submit(() -> structure.createNode(service, "service")).join().id();
        return structure.submit(() -> structure.addInput(node, name, "tcp")).join().id();
    }

    private static void links(final Structure structure, final Consumer<String> out) {
        final List<String> links = links(structure);
        if (links.isEmpty()) {
            out.accept("  no links");
        }
        for (final String link : links) {
            out.accept("  " + link);
        }
    }

    private static List<String> links(final Structure structure) {
        final List<String> links = new ArrayList<>();
        for (final Map<String, Object> link : structure.query("link").join()) {
            final Map<String, Object> from = port(structure, link.get("from"));
            final Map<String, Object> to = port(structure, link.get("to"));
            links.add(nodeName(structure, from) + " -> " + nodeName(structure, to)
                    + " (" + to.get("address") + ")");
        }
        Collections.sort(links);
        return links;
    }

    private static Map<String, Object> port(final Structure structure, final Object id) {
        return structure.snapshotObject((Long) id).join();
    }

    private static Object nodeName(final Structure structure, final Map<String, Object> port) {
        return structure.snapshotObject((Long) port.get("node")).join().get("name");
    }

    private B06Addresses() {
    }
}
