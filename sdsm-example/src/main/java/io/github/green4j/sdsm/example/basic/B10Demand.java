package io.github.green4j.sdsm.example.basic;

import io.github.green4j.sdsm.DetailLevel;
import io.github.green4j.sdsm.Emit;
import io.github.green4j.sdsm.EventLoop;
import io.github.green4j.sdsm.Feed;
import io.github.green4j.sdsm.Interest;
import io.github.green4j.sdsm.LoopGroup;
import io.github.green4j.sdsm.Materializer;
import io.github.green4j.sdsm.Observation;
import io.github.green4j.sdsm.Source;
import io.github.green4j.sdsm.Structure;
import io.github.green4j.sdsm.StructureRuntime;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Demand runs the other way: a display says how much it wants of what, and the source that
 * pays for what it fetches hears it. A source names what is not worth having at every level;
 * a value nobody wants at that level is taken off the structure at once and not written again
 * until someone does.
 * <p>
 * Demand is the highest level any client asks for, so what one window has put away stays
 * whole while another is showing it.
 */
public final class B10Demand {

    private static final int SOURCE = 1;
    private static final int SCOPE = 0;

    /**
     * One stream as the metrics service sees it: whether it is flowing is cheap to ask, how
     * much it holds is not.
     */
    static final class Stream implements Observation {
        private final String id;
        private final String version;
        private final String status;
        private final long backlog;

        Stream(final String id, final String version, final String status, final long backlog) {
            this.id = id;
            this.version = version;
            this.status = status;
            this.backlog = backlog;
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
     * The metrics service. It says backlog is worth fetching only for what is looked at
     * closely, and writes down what it is told about each stream.
     */
    static final class Metrics implements Source<Stream> {
        private final List<String> told = Collections.synchronizedList(new ArrayList<>());
        private Feed<Stream> feed;
        private volatile int offered;
        private volatile int released;

        @Override
        public void start(final Feed<Stream> started) {
            feed = started;
        }

        @Override
        public void stop() {
        }

        @Override
        public Map<String, DetailLevel> suppliedProperties() {
            return Collections.singletonMap("backlog", DetailLevel.FINE);
        }

        @Override
        public void detailLevelChanged(final CharSequence externalId, final DetailLevel level) {
            told.add(externalId + " " + level);
        }

        @Override
        public void release(final Stream observation) {
            released++;
        }

        /**
         * @param streams every stream there is
         */
        void sees(final Stream... streams) {
            for (final Stream stream : streams) {
                offered++;
                feed.observed(stream);
            }
            feed.complete(SCOPE);
            Show.await(() -> released == offered);
        }

        /**
         * @param count how many answers are expected by now
         * @param out   where to say what the source was told
         */
        void told(final int count, final Consumer<String> out) {
            Show.await(() -> told.size() >= count);
            final List<String> sorted;
            synchronized (told) {
                sorted = new ArrayList<>(told);
                told.clear();
            }
            Collections.sort(sorted);
            for (final String line : sorted) {
                out.accept("  the source is told: " + line);
            }
        }
    }

    static final class Layout implements Materializer<Stream> {
        @Override
        public void materialize(final Stream stream, final Emit emit) {
            final long node = emit.node(stream.id, stream.id, "stream");
            emit.setText(node, emit.keys().idOf("status"), stream.status);
            emit.setLong(node, emit.keys().idOf("backlog"), stream.backlog);
        }
    }

    public static void main(final String[] args) {
        run(System.out::println);
    }

    static void run(final Consumer<String> out) {
        try (StructureRuntime runtime = StructureRuntime.create(1, 1, "demand")) {
            final Structure structure = runtime.newStructure();
            try (LoopGroup loops = LoopGroup.create(structure, 1, "demand")) {
                script(structure, loops.newLoop(), out);
            }
        }
    }

    private static void script(final Structure structure,
                               final EventLoop loop,
                               final Consumer<String> out) {
        final Metrics metrics = new Metrics();
        loop.attach(SOURCE, metrics, new Layout());

        // Until a client speaks, everything is wanted whole.
        out.accept("-- nobody has said what they want");
        metrics.sees(new Stream("orders", "1", "FLOWING", 100L),
                new Stream("trades", "1", "FLOWING", 20L));
        print(structure, out);

        out.accept("-- the screen folds everything away");
        structure.setInterest("screen", Interest.of(DetailLevel.COARSE)).join();
        metrics.told(2, out);
        print(structure, out);

        out.accept("-- the source answers again; the backlog is not written");
        metrics.sees(new Stream("orders", "2", "FLOWING", 110L),
                new Stream("trades", "2", "STALLED", 25L));
        print(structure, out);

        final long orders = structure.matchedObjectIds("node[name=orders]").join()[0];
        out.accept("-- the screen opens orders");
        structure.setInterest("screen", Interest.of(DetailLevel.COARSE)
                .at(orders, DetailLevel.FINE)).join();
        metrics.told(1, out);
        metrics.sees(new Stream("orders", "3", "FLOWING", 120L),
                new Stream("trades", "3", "STALLED", 30L));
        print(structure, out);

        out.accept("-- a wall screen wants everything");
        structure.setInterest("wall", Interest.of(DetailLevel.FINE)).join();
        metrics.told(1, out);
        metrics.sees(new Stream("orders", "4", "FLOWING", 125L),
                new Stream("trades", "4", "STALLED", 35L));
        print(structure, out);

        out.accept("-- the wall screen is switched off");
        structure.clearInterest("wall").join();
        metrics.told(1, out);
        print(structure, out);
    }

    private static void print(final Structure structure, final Consumer<String> out) {
        for (final Map<String, Object> stream : structure.query("node[type=stream]").join()) {
            out.accept("  " + stream.get("name") + " " + stream.get("status")
                    + " backlog=" + stream.getOrDefault("backlog", "-"));
        }
    }

    private B10Demand() {
    }
}
