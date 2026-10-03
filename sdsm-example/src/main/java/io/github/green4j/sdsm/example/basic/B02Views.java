package io.github.green4j.sdsm.example.basic;

import io.github.green4j.sdsm.DeliveryPolicy;
import io.github.green4j.sdsm.Input;
import io.github.green4j.sdsm.Node;
import io.github.green4j.sdsm.Output;
import io.github.green4j.sdsm.Structure;
import io.github.green4j.sdsm.StructureRuntime;
import io.github.green4j.sdsm.View;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * How anyone outside learns what the structure holds: a view selects objects, and its
 * subscriber is told first everything the view holds, then every change to it. An object
 * that stops matching leaves the view the way a removed one does.
 */
public final class B02Views {

    public static void main(final String[] args) {
        run(System.out::println);
    }

    static void run(final Consumer<String> out) {
        try (StructureRuntime runtime = StructureRuntime.create(1, 1, "views")) {
            final Structure structure = runtime.newStructure();
            final int ready = structure.propertyKeys().idOf("ready");

            // The services, with the links between them and the ports those links join.
            final View services = structure.createView("services",
                    "node[type=service], between(node[type=service])",
                    null, DeliveryPolicy.onChange()).join();
            // Only what is ready, and of it only whether it is: what an object is - its name,
            // its kind - comes whatever the view asks for.
            final View readyOnes = structure.createView("ready", "node[ready=true]",
                    Collections.singleton("ready"), DeliveryPolicy.onChange()).join();
            // One node, and every link touching it, wherever the other end is.
            final View aroundIngest = structure.createView("around ingest",
                    "node[name=ingest], touching(node[name=ingest])",
                    Collections.singleton("ready"), DeliveryPolicy.onChange()).join();

            out.accept("-- subscribed");
            structure.subscribe(services.id(), batch -> Show.batch("services", batch, out))
                    .join();
            structure.subscribe(readyOnes.id(), batch -> Show.batch("ready", batch, out))
                    .join();
            structure.subscribe(aroundIngest.id(),
                    batch -> Show.batch("around ingest", batch, out)).join();

            out.accept("-- two services and a link");
            final long[] store = new long[1];
            structure.run(() -> {
                final Node ingest = structure.createNode("ingest", "service");
                final Node sink = structure.createNode("store", "service");
                final Output output = structure.addOutput(ingest.id(), "out", "tcp");
                final Input input = structure.addInput(sink.id(), "in", "tcp");
                structure.createLink("ingest-store", "tcp", output.id(), input.id());
                structure.setBoolean(ingest.id(), ready, true);
                structure.setBoolean(sink.id(), ready, true);
                store[0] = sink.id();
            }).join();

            out.accept("-- the store is not ready");
            structure.run(() -> structure.setBoolean(store[0], ready, false)).join();

            // A new selector is a new view: its subscribers are told the whole of it again.
            out.accept("-- the ready view asks for what is not ready");
            structure.setViewSelector(readyOnes.id(), "node[ready=false]").join();

            out.accept("-- the store is removed");
            structure.run(() -> structure.remove(store[0])).join();

            // A selector answers once, without a view, too.
            out.accept("-- a job that says nothing about being ready");
            structure.run(() -> structure.createNode("audit", "job")).join();
            // What has a value at all.
            show(structure, "node[ready]", out);
            // What is not ready - what does not say among it, which [ready!=true] would miss.
            show(structure, "node & ![ready=true]", out);
            // The ports of a node, linked or not.
            show(structure, "on(node[name=ingest])", out);
        }
    }

    private static void show(final Structure structure, final String selector, final Consumer<String> out) {
        final List<String> names = new ArrayList<>();
        for (final Map<String, Object> object : structure.query(selector).join()) {
            names.add(object.get("kind") + " " + object.get("name"));
        }
        Collections.sort(names);
        out.accept(selector + " " + names);
    }

    private B02Views() {
    }
}
