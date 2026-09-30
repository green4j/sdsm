package io.github.green4j.sdsm.example.demo;

import io.github.green4j.sdsm.DetailLevel;
import io.github.green4j.sdsm.Emit;
import io.github.green4j.sdsm.Feed;
import io.github.green4j.sdsm.Fold;
import io.github.green4j.sdsm.Materializer;
import io.github.green4j.sdsm.Observation;
import io.github.green4j.sdsm.Over;
import io.github.green4j.sdsm.Source;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * How the shop gets into the structure: what a service looks like when it is seen, the watch
 * that sees them, and what a service is in the structure.
 */
public final class Shop {

    public static final String SERVICE = "service";
    public static final String TIER = "tier";

    private static final int SCOPE = 0;

    /**
     * One service as the watch saw it. Services never name each other: each says what it can
     * be reached at and what it calls, and the structure joins them.
     */
    public static final class Service implements Observation {
        private final String name;
        private final String tier;
        private final boolean up;
        private final int replicas;
        private final int ready;
        private final long rps;
        private final List<String> calls;
        private final String version;

        Service(final String name, final String tier, final boolean up, final int replicas,
                final int ready, final long rps, final List<String> calls) {
            this.name = name;
            this.tier = tier;
            this.up = up;
            this.replicas = replicas;
            this.ready = ready;
            this.rps = rps;
            this.calls = calls;
            this.version = up + "/" + replicas + "/" + ready + "/" + rps + "/" + calls;
        }

        public String name() {
            return name;
        }

        @Override
        public CharSequence externalId() {
            return name;
        }

        /**
         * @return everything seen, so a service seen the same as last time is not written again
         */
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
     * The watch. It is told what the world looks like and passes it on, all of it every time,
     * so a service no longer there is swept away.
     */
    public static final class Watch implements Source<Service> {
        private Feed<Service> feed;
        private volatile int offered;
        private volatile int released;

        @Override
        public void start(final Feed<Service> started) {
            feed = started;
        }

        @Override
        public void stop() {
        }

        /**
         * Requests per second are worth fetching only for what someone looks at closely.
         */
        @Override
        public Map<String, DetailLevel> suppliedProperties() {
            return Collections.singletonMap("rps", DetailLevel.FINE);
        }

        @Override
        public void release(final Service observation) {
            released++;
        }

        /**
         * @param services every service there is
         */
        public void sees(final List<Service> services) {
            for (final Service service : services) {
                offered++;
                feed.observed(service);
            }
            feed.complete(SCOPE);
        }

        /**
         * @return whether everything seen has been written
         */
        public boolean settled() {
            return released == offered;
        }
    }

    /**
     * A service is a node in the group of its tier, with an input reachable at its name and
     * an output for each service it calls. The tier sums the requests of its services and
     * counts those that are down.
     */
    public static final class Layout implements Materializer<Service> {
        private int status = -1;
        private int replicas;
        private int ready;
        private int rps;
        private int down;

        @Override
        public void materialize(final Service service, final Emit emit) {
            resolve(emit);
            final long tier = emit.node("tier:" + service.tier, service.tier, TIER);
            emit.derive(tier, rps, Fold.SUM, Over.children(rps, SERVICE));
            emit.derive(tier, down, Fold.SUM, Over.children(down, SERVICE));

            final long node = emit.node(service.name, service.name, SERVICE);
            emit.contain(tier, node, TIER);
            emit.setText(node, status, service.up ? "UP" : "DOWN");
            emit.setLong(node, replicas, service.replicas);
            emit.setLong(node, ready, service.ready);
            emit.setLong(node, rps, service.rps);
            emit.setLong(node, down, service.up ? 0L : 1L);

            emit.provide(emit.input(node, service.name + "<", "in", "http"), service.name);
            for (final String callee : service.calls) {
                emit.require(emit.output(node, service.name + ">" + callee, callee, "http"),
                        callee);
            }
        }

        private void resolve(final Emit emit) {
            if (status >= 0) {
                return;
            }
            status = emit.keys().idOf("status");
            replicas = emit.keys().idOf("replicas");
            ready = emit.keys().idOf("ready");
            rps = emit.keys().idOf("rps");
            down = emit.keys().idOf("down");
        }
    }

    private Shop() {
    }
}
