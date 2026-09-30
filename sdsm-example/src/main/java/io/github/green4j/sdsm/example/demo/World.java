package io.github.green4j.sdsm.example.demo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * A small web shop that lives on its own: load wanders every tick, and now and then something
 * happens - a service falls over and comes back, one is scaled out, one is deployed, one is
 * taken away. The script repeats every {@link #CYCLE} ticks.
 */
public final class World {

    public static final int CYCLE = 80;

    private static final class Running {
        private final String name;
        private final String tier;
        private final long baseRps;
        private List<String> calls;
        private boolean up = true;
        private int replicas;
        private int ready;
        private long rps;

        Running(final String name, final String tier, final int replicas, final long baseRps,
                final String... calls) {
            this.name = name;
            this.tier = tier;
            this.replicas = replicas;
            this.ready = replicas;
            this.baseRps = baseRps;
            this.rps = baseRps;
            this.calls = Arrays.asList(calls);
        }
    }

    private final Map<String, Running> services = new LinkedHashMap<>();
    private final Random random = new Random(42L);
    private long tick;
    private String event;

    public World() {
        deploy(new Running("gateway", "edge", 3, 600L, "catalog", "cart", "orders"));
        deploy(new Running("catalog", "shop", 2, 400L, "stock"));
        deploy(new Running("cart", "shop", 2, 230L, "orders"));
        deploy(new Running("orders", "shop", 3, 180L, "payments", "stock"));
        deploy(new Running("payments", "shop", 2, 170L));
        deploy(new Running("stock", "shop", 2, 380L));
    }

    /**
     * Moves the shop on one tick.
     */
    public void tick() {
        tick++;
        final String happened = happen((int) (tick % CYCLE));
        if (happened != null) {
            event = "tick " + tick + ": " + happened;
        }
        for (final Running service : services.values()) {
            if (service.ready < service.replicas) {
                service.ready++;            // a pod a tick
            } else if (service.ready > service.replicas) {
                service.ready = service.replicas;
            }
            service.rps = service.up
                    ? service.baseRps * service.ready / Math.max(service.replicas, 1)
                        + random.nextInt(41) - 20
                    : 0L;
        }
    }

    public long now() {
        return tick;
    }

    /**
     * @return the last thing that happened, or null if nothing has yet
     */
    public String event() {
        return event;
    }

    /**
     * @return every service as it is now
     */
    public List<Shop.Service> services() {
        final List<Shop.Service> seen = new ArrayList<>(services.size());
        for (final Running service : services.values()) {
            seen.add(new Shop.Service(service.name, service.tier, service.up, service.replicas,
                    service.up ? service.ready : 0, service.rps, service.calls));
        }
        return seen;
    }

    private String happen(final int at) {
        switch (at) {
            case 10:
                services.get("payments").up = false;
                return "payments is down";
            case 18:
                services.get("payments").up = true;
                return "payments is back";
            case 26:
                services.get("stock").replicas = 4;
                return "stock scales out to 4";
            case 40:
                deploy(new Running("reviews", "shop", 1, 40L, "stock"));
                services.get("catalog").calls = Arrays.asList("stock", "reviews");
                return "reviews is deployed, catalog starts calling it";
            case 55:
                services.remove("cart");
                return "cart is taken away";
            case 65:
                deploy(new Running("cart", "shop", 2, 230L, "orders"));
                return "cart is back";
            case 75:
                services.remove("reviews");
                services.get("catalog").calls = Collections.singletonList("stock");
                return "reviews is taken away";
            case 0:
                services.get("stock").replicas = 2;
                return "stock scales in to 2";
            default:
                return null;
        }
    }

    private void deploy(final Running service) {
        service.ready = 0;
        services.put(service.name, service);
    }
}
