package io.github.green4j.sdsm.example.demo;

import io.github.green4j.sdsm.DeliveryPolicy;
import io.github.green4j.sdsm.example.basic.Show;
import io.github.green4j.sdsm.LoopGroup;
import io.github.green4j.sdsm.Structure;
import io.github.green4j.sdsm.StructureRuntime;
import io.github.green4j.sdsm.View;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * The shop on a terminal. The world moves, the watch tells the structure, the structure tells
 * a view, and the screen is drawn from what the view said: nothing on it is read from the
 * structure directly.
 */
public final class Demo implements AutoCloseable {

    private static final long TICK_NANOS = TimeUnit.MILLISECONDS.toNanos(500L);
    private static final String CLEAR = "\u001b[H\u001b[2J";

    private final StructureRuntime runtime = StructureRuntime.create(1, 1, "demo");
    private final Structure structure = runtime.newStructure();
    private final LoopGroup loops = LoopGroup.create(structure, 1, "demo");
    private final World world = new World();
    private final Shop.Watch watch = new Shop.Watch();
    private final ClientView view = new ClientView();

    public Demo() {
        loops.newLoop().attach(1, watch, new Shop.Layout());
        final View everything = structure.createView("shop", "*", null,
                DeliveryPolicy.onChange()).join();
        structure.subscribe(everything.id(), view).join();
    }

    public static void main(final String[] args) {
        try (Demo demo = new Demo()) {
            final Screen screen = new Screen(demo.view, true);
            while (true) {
                demo.tick();
                System.out.print(CLEAR);
                System.out.print(screen.render(demo.world.now(), demo.world.event()));
                LockSupport.parkNanos(TICK_NANOS);
            }
        }
    }

    /**
     * Moves the world on one tick and waits until the view has said what it came to.
     */
    public void tick() {
        world.tick();
        watch.sees(world.services());
        Show.await(watch::settled);
        structure.flushAll().join();
    }

    public World world() {
        return world;
    }

    public Structure structure() {
        return structure;
    }

    public ClientView view() {
        return view;
    }

    @Override
    public void close() {
        loops.close();
        runtime.close();
    }
}
