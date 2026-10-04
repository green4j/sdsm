package io.github.green4j.sdsm.example.basic;

import io.github.green4j.sdsm.Emit;
import io.github.green4j.sdsm.EventLoop;
import io.github.green4j.sdsm.Feed;
import io.github.green4j.sdsm.FeedState;
import io.github.green4j.sdsm.LoopGroup;
import io.github.green4j.sdsm.Materializer;
import io.github.green4j.sdsm.Observation;
import io.github.green4j.sdsm.Source;
import io.github.green4j.sdsm.Structure;
import io.github.green4j.sdsm.StructureRuntime;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * How the world gets into a structure: a source says what it sees, a materializer says what
 * that is in the graph, and the feed between them decides what is new, what is gone and what
 * the source's silence means. The world here is three pods and a watch on them.
 * <p>
 * The feed runs on a loop thread of its own and writes on the structure's, so the caller
 * waits for each round to show before it looks.
 */
public final class B09Feed {

    private static final int SOURCE = 1;
    private static final int SCOPE = 0;

    /**
     * One pod as the watch saw it. The version is what the world calls this state of it: an
     * observation repeating a version already written is not written again.
     */
    static final class Pod implements Observation {
        private final String id;
        private final String version;
        private final String status;

        Pod(final String id, final String version, final String status) {
            this.id = id;
            this.version = version;
            this.status = status;
        }

        @Override
        public CharSequence externalId() {
            return id;
        }

        @Override
        public CharSequence version() {
            return version;
        }

        @Override
        public int scope() {
            return SCOPE;
        }
    }

    /**
     * The watch. It owns no polling: the caller says when it sees and what.
     */
    static final class Watch implements Source<Pod> {
        private Feed<Pod> feed;

        @Override
        public void start(final Feed<Pod> started) {
            feed = started;
        }

        @Override
        public void stop() {
        }

        @Override
        public void rejected(final Pod observation, final Throwable reason) {
            Show.rejected(observation, reason);
        }

        /**
         * Says every pod there is, and that this is all of them.
         *
         * @param pods every pod there is
         */
        void sees(final Pod... pods) {
            for (final Pod pod : pods) {
                feed.observed(pod);
            }
            feed.complete(SCOPE);
        }

        void blind() {
            feed.unavailable(new IllegalStateException("the API server does not answer"));
        }

        void back() {
            feed.available();
        }
    }

    /**
     * Lays a pod out as a node in the cluster group, and writes where the watch stands on the
     * cluster: a watch that cannot see says nothing about the pods, so what its silence means
     * has to be written somewhere a display can read it.
     */
    static final class Layout implements Materializer<Pod> {
        private final Consumer<String> out;

        Layout(final Consumer<String> out) {
            this.out = out;
        }

        @Override
        public void materialize(final Pod pod, final Emit emit) {
            out.accept("  materialize " + pod.id + " " + pod.version + " " + pod.status);
            final long node = emit.node(pod.id, pod.id, "pod");
            emit.contain(cluster(emit), node, "placement");
            emit.setText(node, emit.keys().idOf("status"), pod.status);
        }

        @Override
        public void stateChanged(final FeedState state, final Throwable reason, final Emit emit) {
            out.accept("  feed " + state + (reason != null ? ": " + reason.getMessage() : ""));
            emit.setText(cluster(emit), emit.keys().idOf("watch"), state.name());
        }

        private static long cluster(final Emit emit) {
            return emit.node("cluster", "cluster", "cluster");
        }
    }

    public static void main(final String[] args) {
        run(System.out::println);
    }

    /**
     * @param out where each line goes: what the materializer was asked to do, and what the
     *            structure holds after each round
     */
    static void run(final Consumer<String> out) {
        try (StructureRuntime runtime = StructureRuntime.create(1, 1, "feed")) {
            final Structure structure = runtime.newStructure();
            try (LoopGroup loops = LoopGroup.create(structure, 1, "feed")) {
                script(structure, loops.newLoop(), out);
            }
        }
    }

    private static void script(final Structure structure,
                               final EventLoop loop,
                               final Consumer<String> out) {
        final Watch watch = new Watch();
        final Feed<Pod> feed = loop.attach(SOURCE, watch, new Layout(out));

        out.accept("the watch sees three pods");
        watch.sees(new Pod("pod-a", "1", "Running"),
                new Pod("pod-b", "1", "Pending"),
                new Pod("pod-c", "1", "Running"));
        Show.await(() -> feed.state() == FeedState.CONVERGED);
        print(structure, out);

        // pod-a has not changed and is not written again; pod-c is not in the round, and
        // the round is all of them, so it is gone.
        out.accept("pod-b starts, pod-c is deleted");
        watch.sees(new Pod("pod-a", "1", "Running"),
                new Pod("pod-b", "2", "Running"));
        Show.await(() -> pods(structure).size() == 2);
        print(structure, out);

        // Not seeing is not seeing nothing: the pods stay, and the cluster says why they
        // may be out of date.
        out.accept("the watch goes blind");
        watch.blind();
        Show.await(() -> feed.state() == FeedState.STALE);
        print(structure, out);

        // Back, it says the whole world again; what it said before is not written twice.
        out.accept("the watch is back, and pod-d has been started meanwhile");
        watch.back();
        watch.sees(new Pod("pod-a", "1", "Running"),
                new Pod("pod-b", "2", "Running"),
                new Pod("pod-d", "1", "Pending"));
        Show.await(() -> pods(structure).size() == 3);
        print(structure, out);
    }

    private static void print(final Structure structure, final Consumer<String> out) {
        for (final Map<String, Object> pod : pods(structure)) {
            out.accept("  " + pod.get("$name") + " " + pod.get("status"));
        }
        final Map<String, Object> cluster = structure.query("node[$type=cluster]").join().get(0);
        out.accept("  cluster watch=" + cluster.get("watch"));
    }

    private static List<Map<String, Object>> pods(final Structure structure) {
        return structure.query("node[$type=pod]").join();
    }

    private B09Feed() {
    }
}
