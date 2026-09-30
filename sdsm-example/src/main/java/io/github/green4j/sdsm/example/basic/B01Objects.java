package io.github.green4j.sdsm.example.basic;

import io.github.green4j.sdsm.Input;
import io.github.green4j.sdsm.Link;
import io.github.green4j.sdsm.Node;
import io.github.green4j.sdsm.ObjectKind;
import io.github.green4j.sdsm.Output;
import io.github.green4j.sdsm.PropertyKeys;
import io.github.green4j.sdsm.Structure;
import io.github.green4j.sdsm.StructureRuntime;

import java.util.Arrays;
import java.util.function.Consumer;

/**
 * What a structure holds: nodes, the ports on them, links from an output to an input, and
 * properties on any of these. The structure runs on a thread of its own: it is changed inside
 * a task handed to it with submit(...) or run(...), and read from any thread, a read answering
 * with a future. Here each is waited for.
 */
public final class B01Objects {

    public static void main(final String[] args) {
        run(System.out::println);
    }

    static void run(final Consumer<String> out) {
        try (StructureRuntime runtime = StructureRuntime.create(1, 1, "objects")) {
            final Structure structure = runtime.newStructure();

            final Node ingest = structure.submit(() -> structure.createNode("ingest", "service")).join();
            final Node store = structure.submit(() -> structure.createNode("store", "service")).join();
            final Output output = structure.submit(() -> structure.addOutput(ingest.id(), "out", "tcp")).join();
            final Input input = structure.submit(() -> structure.addInput(store.id(), "in", "tcp")).join();
            final Link link = structure.submit(() -> structure.createLink("ingest-store", "tcp",
                    output.id(), input.id())).join();

            // A property name is turned into an int once; everything after carries the int.
            // Values are typed and written on the structure's thread, which run(...) is.
            final PropertyKeys keys = structure.propertyKeys();
            final int replicas = keys.idOf("replicas");
            final int load = keys.idOf("load");
            final int ready = keys.idOf("ready");
            final int version = keys.idOf("version");
            final int rate = keys.idOf("rate");
            structure.run(() -> {
                structure.setLong(ingest.id(), replicas, 3L);
                structure.setDouble(ingest.id(), load, 0.75);
                structure.setBoolean(ingest.id(), ready, true);
                structure.setText(ingest.id(), version, "1.4.2");
                structure.setBoolean(store.id(), ready, false);
                structure.setDouble(link.id(), rate, 1200.0);
            }).join();

            out.accept("ingest " + structure.snapshotObject(ingest.id()).join());
            out.accept("output " + structure.snapshotObject(output.id()).join());
            out.accept("link   " + structure.snapshotObject(link.id()).join());
            final long[] ends = structure.linkEndpoints(link.id()).join();
            out.accept("the link runs from output " + ends[0] + " of node " + ends[1]
                    + " to input " + ends[2] + " of node " + ends[3]);
            out.accept("outputs of ingest " + Arrays.toString(
                    structure.nodePorts(ingest.id(), ObjectKind.OUTPUT).join()));

            // A selector picks objects by kind and property; the same language selects a view.
            out.accept("ready services " + structure.query("node[ready=true]").join());

            structure.run(() -> structure.removeProperty(ingest.id(), version)).join();
            out.accept("without a version " + structure.snapshotObject(ingest.id()).join());

            // Removing a node takes its ports with it, and every link touching them.
            structure.run(() -> structure.remove(store.id())).join();
            out.accept("after the store is removed " + Arrays.toString(
                    structure.matchedObjectIds("*").join()));
        }
    }

    private B01Objects() {
    }
}
