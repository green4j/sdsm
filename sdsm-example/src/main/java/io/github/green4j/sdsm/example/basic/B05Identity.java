package io.github.green4j.sdsm.example.basic;

import io.github.green4j.sdsm.Node;
import io.github.green4j.sdsm.Output;
import io.github.green4j.sdsm.Structure;
import io.github.green4j.sdsm.StructureObject;
import io.github.green4j.sdsm.StructureRuntime;

import java.util.concurrent.CompletionException;
import java.util.function.Consumer;

/**
 * An object has two ids. The structure's is a number it hands out and never reuses; the
 * external id is what the modelled thing is called in the world that owns it - a pod's uid,
 * a deployment's name - and is how a writer finds the object again. A node is given it when it
 * is made, or never; a port of such a node is known by the node's, its side and its name -
 * {@code uid-1>out}. One external id stands for one object at a time.
 */
public final class B05Identity {

    public static void main(final String[] args) {
        run(System.out::println);
    }

    static void run(final Consumer<String> out) {
        try (StructureRuntime runtime = StructureRuntime.create(1, 1, "identity")) {
            final Structure structure = runtime.newStructure();

            final Node pod = structure.submit(() -> structure.createNode("ingest-7f9c", "pod", "uid-1")).join();
            final Output port = structure.submit(() -> structure.addOutput(pod.id(), "out", "tcp")).join();
            out.accept("pod " + structure.snapshotObject(pod.id()).join());
            out.accept("port " + structure.snapshotObject(port.id()).join());

            // Found by what the world calls it, on the structure's thread.
            out.accept("uid-1 is " + find(structure, "uid-1"));
            out.accept("uid-1>out is " + find(structure, "uid-1>out"));
            out.accept("uid-2 is " + find(structure, "uid-2"));

            out.accept("-- the same uid again");
            try {
                structure.submit(() -> structure.createNode("ingest-7f9c", "pod", "uid-1")).join();
            } catch (final CompletionException e) {
                out.accept("refused: " + e.getCause().getMessage());
            }

            // The pod is recreated under the same uid: a new object, with a new number.
            out.accept("-- the pod is removed and made again");
            structure.run(() -> structure.remove(pod.id())).join();
            out.accept("uid-1 is " + find(structure, "uid-1"));
            out.accept("uid-1>out is " + find(structure, "uid-1>out"));
            structure.submit(() -> structure.createNode("ingest-7f9c", "pod", "uid-1")).join();
            out.accept("uid-1 is " + find(structure, "uid-1"));
        }
    }

    private static String find(final Structure structure, final String externalId) {
        return structure.submit(() -> {
            final StructureObject found = structure.findByExternalId(externalId);
            return found == null ? "nothing" : found.toString();
        }).join();
    }

    private B05Identity() {
    }
}
