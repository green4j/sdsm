package io.github.green4j.sdsm.example.basic;

import io.github.green4j.sdsm.DeliveryPolicy;
import io.github.green4j.sdsm.Input;
import io.github.green4j.sdsm.Node;
import io.github.green4j.sdsm.Output;
import io.github.green4j.sdsm.Structure;
import io.github.green4j.sdsm.StructureRuntime;
import io.github.green4j.sdsm.View;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;

/**
 * Nodes hold nodes along an axis: regions holding clusters holding services along one, the
 * same services banded by stage along another. On one axis a node has one parent at most and
 * the nesting has no cycles; across axes it has as many as there are.
 */
public final class B04Contain {

    public static void main(final String[] args) {
        run(System.out::println);
    }

    static void run(final Consumer<String> out) {
        try (StructureRuntime runtime = StructureRuntime.create(1, 1, "contain")) {
            final Structure structure = runtime.newStructure();

            // Containment is a record of its own, naming the child and the parent, so a
            // subscriber can keep the tree without asking the structure.
            final View everything = structure.createView("everything",
                    "node, link", Collections.emptySet(),
                    DeliveryPolicy.onChange()).join();
            structure.subscribe(everything.id(), batch -> Show.batch("view", batch, out)).join();

            out.accept("-- two services, a link, and what holds them");
            final Node ingest = structure.submit(() -> structure.createNode("ingest", "service")).join();
            final Node store = structure.submit(() -> structure.createNode("store", "service")).join();
            final Output output = structure.submit(() -> structure.addOutput(ingest.id(), "out", "tcp")).join();
            final Input input = structure.submit(() -> structure.addInput(store.id(), "in", "tcp")).join();
            structure.submit(() -> structure.createLink("ingest-store", "tcp", output.id(), input.id())).join();
            final Node eu = structure.submit(() -> structure.createNode("eu", "region")).join();
            final Node euDe = structure.submit(() -> structure.createNode("eu-de-1", "cluster")).join();
            final Node ingestStage = structure.submit(() -> structure.createNode("ingest", "stage")).join();
            structure.run(() -> {
                structure.contain(eu.id(), euDe.id(), "placement");
                structure.contain(euDe.id(), ingest.id(), "placement");
                structure.contain(euDe.id(), store.id(), "placement");
                structure.contain(ingestStage.id(), ingest.id(), "stage");
            }).join();

            out.accept("children of eu-de-1 " + Arrays.toString(
                    structure.children(euDe.id()).join()));
            out.accept("parents of ingest " + Arrays.toString(
                    structure.parents(ingest.id()).join()));

            out.accept("-- what the structure refuses");
            refused(out, () -> structure.run(() -> structure.contain(euDe.id(), eu.id(), "placement")).join());
            refused(out, () -> structure.run(() -> structure.contain(eu.id(), ingest.id(), "placement")).join());

            out.accept("-- the store leaves the cluster");
            structure.run(() -> structure.uncontain(euDe.id(), store.id())).join();

            // A parent holds, it does not own: removing it leaves what was under it.
            out.accept("-- the cluster is removed");
            structure.run(() -> structure.remove(euDe.id())).join();
            out.accept("parents of ingest " + Arrays.toString(
                    structure.parents(ingest.id()).join()));
        }
    }

    private static void refused(final Consumer<String> out, final Runnable call) {
        try {
            call.run();
            out.accept("allowed");
        } catch (final CompletionException e) {
            out.accept("refused: " + e.getCause().getMessage());
        }
    }

    private B04Contain() {
    }
}
