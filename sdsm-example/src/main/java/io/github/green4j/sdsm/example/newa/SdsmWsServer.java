package io.github.green4j.sdsm.example.newa;

import io.github.green4j.newa.lang.Life;
import io.github.green4j.newa.lang.StdErrChannelErrorHandler;
import io.github.green4j.newa.rest.JsonErrorHandler;
import io.github.green4j.newa.rest.RestApi;
import io.github.green4j.newa.rest.RestApiHandler;
import io.github.green4j.newa.server.NettyServer;
import io.github.green4j.newa.server.NettyServerBuilder;
import io.github.green4j.newa.server.ServerMemoryBudget;
import io.github.green4j.newa.websocket.WsApi;
import io.github.green4j.newa.websocket.WsServer;
import io.github.green4j.newa.websocket.subscriptions.SubscriptionWsApiBuilder;
import io.github.green4j.sdsm.LoopGroup;
import io.github.green4j.sdsm.Structure;
import io.github.green4j.sdsm.StructureRuntime;
import io.github.green4j.sdsm.example.demo.Shop;
import io.github.green4j.sdsm.example.demo.World;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The same model, served over a websocket instead of drawn on a terminal. The join is this
 * package: SDSM knows nothing about newa, newa knows nothing about SDSM, and neither the
 * materializers nor the world change a line.
 * <p>
 * Three things are worth the reading. Delivery runs on the server's own threads - SDSM starts
 * none - so a batch is rendered where it is written out. A session that cannot keep up is
 * skipped and re-synchronized rather than disconnected, which is what lets a structural change
 * be skipped too: a delta is not lost, it is made worthless by the snapshot that follows.
 * And a view is a named, long-lived thing with subscribers on both sides of the join, so it is
 * the entity a client subscribes to.
 */
public final class SdsmWsServer {

    public static final int API_VERSION = 1;
    public static final String HOST = "127.0.0.1";
    public static final int PORT = 9020;

    private static final String VIEW = "topology";

    // What a connection may cost, for the memory budget. The largest frame is a snapshot, and no
    // view is larger than "*", whose snapshot of the shop is 17 KB. A session renders a
    // snapshot into a buffer of its own that grows to twice that, and its interest is bounded by
    // the frame that said it.
    private static final int LARGEST_FRAME = 128 * 1024;
    private static final int MAX_INBOUND_FRAME = 16 * 1024;
    private static final int MAX_REQUEST = 8 * 1024;
    private static final long SESSION_HEAP = 2L * LARGEST_FRAME + 2L * MAX_INBOUND_FRAME;
    private static final long ROUND_MILLIS = 1000L;

    public static void main(final String[] args) throws Exception {
        final Life life = new Life();
        final StructureRuntime runtime = StructureRuntime.create(1, 1, "sdsm");
        final Structure structure = runtime.newStructure();
        final ViewChannel channel = new ViewChannel(structure);

        final ClientFrames frames = new ClientFrames(structure, channel);
        final WsApi api = new SubscriptionWsApiBuilder(API_VERSION)
                .withPathPrefix("sdsm")
                .withTextReceiver(frames)
                .withObservers(frames)
                .withSkipOnBackPressure()   // skip and re-synchronize, never lose
                .build();
        final RestApi views = ViewsApi.of(API_VERSION, channel);
        final ServerMemoryBudget memory = ServerMemoryBudget.builder().build();

        final World world = new World();
        final Shop.Watch watch = new Shop.Watch();
        final LoopGroup loops = LoopGroup.create(structure, 1, "sdsm");
        loops.newLoop().attach(1, watch, new Shop.Layout());

        final ScheduledExecutorService clock = Executors.newSingleThreadScheduledExecutor(
                runnable -> {
                    final Thread thread = new Thread(runnable, "sdsm-world");
                    thread.setDaemon(true);
                    return thread;
                });

        life.run(() -> {
            final NettyServer server = WsServer.of(api)
                    .withCompression()
                    .withMaxContentLength(MAX_REQUEST)
                    .withMaxFramePayloadLength(MAX_INBOUND_FRAME)
                    .withAdditionalMemoryEstimate(SESSION_HEAP, 0L)
                    .withMemoryBudget(memory, LARGEST_FRAME)
                    // the HTTP half of the port: whatever is not the websocket path goes on to this
                    .withHandler(() -> new RestApiHandler(views, new JsonErrorHandler(),
                            new StdErrChannelErrorHandler()))
                    .start(new NettyServerBuilder().port(PORT).host(HOST));

            // Delivery belongs to the host, and the host here is the server: rendering a frame
            // happens on the thread that writes it out. Set before anyone subscribes.
            structure.withDeliveryExecutor(server.workerGroup());

            channel.publish(VIEW, "*", null, Duration.ofMillis(100L)).join();
            channel.start();

            clock.scheduleWithFixedDelay(() -> {
                world.tick();
                watch.sees(world.services());
            }, 0L, ROUND_MILLIS, TimeUnit.MILLISECONDS);

            final ServerMemoryBudget.RegistrationSnapshot admitted =
                    server.memoryRegistrationSnapshot();
            System.out.printf("A connection is %d KB of heap and %d KB of direct memory;"
                            + " the budget holds %d KB and %d KB%n",
                    admitted.estimate().heapBytesPerConnection() / 1024,
                    admitted.estimate().directMemoryBytesPerConnection() / 1024,
                    memory.snapshot().heapCapacityBytes() / 1024,
                    memory.snapshot().directMemoryCapacityBytes() / 1024);
            System.out.printf("SDSM on ws://%s:%d%s. Try:%n", HOST, PORT, api.websocketPath());
            System.out.printf("  wscat -c ws://%s:%d%s   then, one frame per line:%n",
                    HOST, PORT, api.websocketPath());
            System.out.println("    {\"op\":\"subscribe\",\"view\":\"topology\"}");
            System.out.println("    {\"op\":\"interest\",\"base\":\"COARSE\"}"
                    + "   -> rps stops being written");
            System.out.println("    {\"op\":\"interest\",\"base\":\"COARSE\","
                    + "\"overrides\":{\"payments\":\"FINE\"}}   -> except for payments");
            System.out.println("    {\"op\":\"unsubscribe\",\"view\":\"topology\"}");
            System.out.printf("  curl -s http://%s:%d/v1/views%n", HOST, PORT);
            System.out.printf("  curl -s http://%s:%d/v1/views -d name=services"
                    + " --data-urlencode 'selector=node[type=service]' -d keys=status%n", HOST, PORT);
            System.out.printf("  curl -s -X DELETE http://%s:%d/v1/views/services%n", HOST, PORT);
            return server;
        });

        clock.shutdownNow();
        channel.close();
        loops.close();
        runtime.close();
    }

    private SdsmWsServer() {
    }
}
