package io.github.green4j.sdsm.example.basic;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Each step says what it did line by line, and says the same every time.
 */
class StepsTest {

    private static final List<String> B01_OBJECTS = List.of(
            "ingest {id=0, name=ingest, type=service, kind=NODE, replicas=3, load=0.75, ready"
                    + "=true, version=1.4.2}",
            "output {id=2, name=out, type=tcp, kind=OUTPUT, node=0, links=1}",
            "link   {id=4, name=ingest-store, type=tcp, kind=LINK, from=2, to=3, rate=1200.0}",
            "the link runs from output 2 of node 0 to input 3 of node 1",
            "outputs of ingest [2]",
            "ready services [{id=0, name=ingest, type=service, kind=NODE, replicas=3, load=0."
                    + "75, ready=true, version=1.4.2}]",
            "without a version {id=0, name=ingest, type=service, kind=NODE, replicas=3, load="
                    + "0.75, ready=true}",
            "after the store is removed [0, 2]");

    private static final List<String> B02_VIEWS = List.of(
            "-- subscribed",
            "services snapshot",
            "ready snapshot",
            "around ingest snapshot",
            "-- two services and a link",
            "services batch",
            "  ADDED NODE 3",
            "  ADDED NODE 4",
            "  ADDED LINK 7",
            "  ADDED OUTPUT 5",
            "  ADDED INPUT 6",
            "  3: id=3 name=ingest type=service kind=NODE",
            "  4: id=4 name=store type=service kind=NODE",
            "  7: id=7 name=ingest-store type=tcp kind=LINK from=5 to=6",
            "  5: id=5 name=out type=tcp kind=OUTPUT node=3 links=1",
            "  6: id=6 name=in type=tcp kind=INPUT node=4 links=1",
            "  3: ready=true",
            "  4: ready=true",
            "ready batch",
            "  ADDED NODE 3",
            "  ADDED NODE 4",
            "  3: id=3 name=ingest type=service kind=NODE ready=true",
            "  4: id=4 name=store type=service kind=NODE ready=true",
            "around ingest batch",
            "  ADDED NODE 3",
            "  ADDED LINK 7",
            "  ADDED OUTPUT 5",
            "  ADDED INPUT 6",
            "  3: id=3 name=ingest type=service kind=NODE",
            "  7: id=7 name=ingest-store type=tcp kind=LINK from=5 to=6",
            "  5: id=5 name=out type=tcp kind=OUTPUT node=3 links=1",
            "  6: id=6 name=in type=tcp kind=INPUT node=4 links=1",
            "  3: ready=true",
            "-- the store is not ready",
            "services batch",
            "  4: ready=false",
            "ready batch",
            "  REMOVED NODE 4",
            "-- the ready view asks for what is not ready",
            "ready snapshot",
            "  ADDED NODE 4",
            "  4: id=4 name=store type=service kind=NODE ready=false",
            "-- the store is removed",
            "services batch",
            "  REMOVED LINK 7",
            "  REMOVED OUTPUT 5",
            "  REMOVED INPUT 6",
            "  REMOVED NODE 4",
            "ready batch",
            "  REMOVED NODE 4",
            "around ingest batch",
            "  REMOVED LINK 7",
            "  REMOVED OUTPUT 5",
            "  REMOVED INPUT 6",
            "-- a job that says nothing about being ready",
            "node[ready] [NODE ingest]",
            "node & ![ready=true] [NODE audit]",
            "on(node[name=ingest]) [OUTPUT out]");

    private static final List<String> B03_TASKS = List.of(
            "nodes snapshot",
            "-- three tasks",
            "nodes batch",
            "  ADDED NODE 1",
            "  1: id=1 name=a type=service kind=NODE",
            "nodes batch",
            "  ADDED NODE 2",
            "  2: id=2 name=b type=service kind=NODE",
            "nodes batch",
            "  1: status=STARTING",
            "-- one task",
            "nodes batch",
            "  ADDED NODE 3",
            "  3: id=3 name=c type=service kind=NODE status=STARTING",
            "  1: status=READY",
            "-- two sources write one node",
            "nodes batch",
            "  2: status=READY",
            "nodes batch",
            "  2: cpu=40",
            "status is written by 1, cpu by 2",
            "-- a task fails",
            "nodes batch",
            "  2: status=FAILED",
            "the task failed: the task broke",
            "-- three writes, held apart",
            "nodes batch",
            "  2: cpu=43",
            "-- one subscriber asks for the whole view",
            "again snapshot",
            "  ADDED NODE 1",
            "  ADDED NODE 2",
            "  ADDED NODE 3",
            "  1: id=1 name=a type=service kind=NODE status=READY",
            "  2: id=2 name=b type=service kind=NODE status=FAILED cpu=43",
            "  3: id=3 name=c type=service kind=NODE status=STARTING");

    private static final List<String> B04_CONTAIN = List.of(
            "view snapshot",
            "-- two services, a link, and what holds them",
            "view batch",
            "  ADDED NODE 1",
            "  1: id=1 name=ingest type=service kind=NODE",
            "view batch",
            "  ADDED NODE 2",
            "  2: id=2 name=store type=service kind=NODE",
            "view batch",
            "  ADDED LINK 5",
            "  ADDED OUTPUT 3",
            "  ADDED INPUT 4",
            "  5: id=5 name=ingest-store type=tcp kind=LINK from=3 to=4",
            "  3: id=3 name=out type=tcp kind=OUTPUT node=1 links=1",
            "  4: id=4 name=in type=tcp kind=INPUT node=2 links=1",
            "view batch",
            "  ADDED NODE 6",
            "  6: id=6 name=eu type=region kind=NODE",
            "view batch",
            "  ADDED NODE 7",
            "  7: id=7 name=eu-de-1 type=cluster kind=NODE",
            "view batch",
            "  ADDED NODE 8",
            "  8: id=8 name=ingest type=stage kind=NODE",
            "view batch",
            "  CONTAINED 7 6",
            "  CONTAINED 1 7",
            "  CONTAINED 2 7",
            "  CONTAINED 1 8",
            "  6: axis=placement",
            "  7: axis=placement",
            "  8: axis=stage",
            "children of eu-de-1 [1, 2]",
            "parents of ingest [7, 8]",
            "-- what the structure refuses",
            "refused: Cycle detected: cannot place '6' inside '7'",
            "refused: Object '1' is already under '7' on axis 'placement'",
            "-- the store leaves the cluster",
            "view batch",
            "  UNCONTAINED 2 7",
            "-- the cluster is removed",
            "view batch",
            "  UNCONTAINED 1 7",
            "  REMOVED NODE 7",
            "parents of ingest [8]");

    private static final List<String> B05_IDENTITY = List.of(
            "pod {id=0, name=ingest-7f9c, type=pod, kind=NODE, externalId=uid-1}",
            "port {id=1, name=out, type=tcp, kind=OUTPUT, externalId=uid-1>out, node=0, links"
                    + "=0}",
            "uid-1 is NODE(0, ingest-7f9c)",
            "uid-1>out is OUTPUT(1, out)",
            "uid-2 is nothing",
            "-- the same uid again",
            "refused: External id 'uid-1' is already held by object 0",
            "-- the pod is removed and made again",
            "uid-1 is nothing",
            "uid-1>out is nothing",
            "uid-1 is NODE(2, ingest-7f9c)");

    private static final List<String> B06_ADDRESSES = List.of(
            "-- the reader looks for orders; nobody provides them yet",
            "  no links",
            "-- the writer provides orders",
            "  writer -> reader (orders)",
            "-- the link cannot be removed, only the declaration",
            "refused: Link 4 is drawn from an address; change the declaration",
            "  no links",
            "-- blue and green, each with a writer and a reader of orders, and one market bot"
                    + "h read prices from",
            "  blue-writer -> blue-reader (orders)",
            "  green-writer -> green-reader (orders)",
            "  market -> blue-reader (prices)",
            "  market -> green-reader (prices)",
            "-- green may read from blue",
            "  blue-writer -> blue-reader (orders)",
            "  blue-writer -> green-reader (orders)",
            "  green-writer -> green-reader (orders)",
            "  market -> blue-reader (prices)",
            "  market -> green-reader (prices)",
            "-- and no longer",
            "  blue-writer -> blue-reader (orders)",
            "  green-writer -> green-reader (orders)",
            "  market -> blue-reader (prices)",
            "  market -> green-reader (prices)",
            "-- an archive reads news and prices through one input",
            "  blue-writer -> blue-reader (orders)",
            "  green-writer -> green-reader (orders)",
            "  market -> archive (prices)",
            "  market -> blue-reader (prices)",
            "  market -> green-reader (prices)",
            "  news -> archive (news)",
            "who reads prices: [archive, blue-reader, green-reader]",
            "-- the archive keeps prices only",
            "  blue-writer -> blue-reader (orders)",
            "  green-writer -> green-reader (orders)",
            "  market -> archive (prices)",
            "  market -> blue-reader (prices)",
            "  market -> green-reader (prices)");

    private static final List<String> B07_DERIVE = List.of(
            "  eu backlog=- (0 of 3) lag=-",
            "  eu-de-1 backlog=- (0 of 2) lag=-",
            "  eu-de-2 backlog=- (0 of 1) lag=-",
            "-- two streams say how far behind they are",
            "  eu backlog=140 (2 of 3) lag=9",
            "  eu-de-1 backlog=100 (1 of 2) lag=3",
            "  eu-de-2 backlog=40 (1 of 1) lag=9",
            "-- and the third",
            "  eu backlog=145 (3 of 3) lag=9",
            "  eu-de-1 backlog=105 (2 of 2) lag=3",
            "  eu-de-2 backlog=40 (1 of 1) lag=9",
            "-- quotes is removed",
            "  eu backlog=105 (2 of 2) lag=3",
            "  eu-de-1 backlog=105 (2 of 2) lag=3",
            "  eu-de-2 backlog=- (0 of 0) lag=-",
            "-- a caller writes the backlog of a cluster",
            "refused: Property 'backlog' of '1' is derived");

    private static final List<String> B08_OWN_KEYS = List.of(
            "  orders broker=- exporter=- backlog=-",
            "  trades broker=- exporter=- backlog=-",
            "  eu-de-1 backlog=-",
            "-- the broker answers",
            "  orders broker=100 exporter=- backlog=100",
            "  trades broker=20 exporter=- backlog=20",
            "  eu-de-1 backlog=120",
            "-- the exporter answers, and sees more on orders",
            "  orders broker=100 exporter=130 backlog=130",
            "  trades broker=20 exporter=10 backlog=20",
            "  eu-de-1 backlog=150",
            "-- the exporter no longer answers on orders",
            "  orders broker=100 exporter=- backlog=100",
            "  trades broker=20 exporter=10 backlog=20",
            "  eu-de-1 backlog=120");

    private static final List<String> B09_FEED = List.of(
            "the watch sees three pods",
            "  materialize pod-a 1 Running",
            "  materialize pod-b 1 Pending",
            "  materialize pod-c 1 Running",
            "  feed CONVERGED",
            "  pod-a Running",
            "  pod-b Pending",
            "  pod-c Running",
            "  cluster watch=CONVERGED",
            "pod-b starts, pod-c is deleted",
            "  materialize pod-b 2 Running",
            "  pod-a Running",
            "  pod-b Running",
            "  cluster watch=CONVERGED",
            "the watch goes blind",
            "  feed STALE: the API server does not answer",
            "  pod-a Running",
            "  pod-b Running",
            "  cluster watch=STALE",
            "the watch is back, and pod-d has been started meanwhile",
            "  feed CONVERGED",
            "  materialize pod-d 1 Pending",
            "  pod-a Running",
            "  pod-b Running",
            "  pod-d Pending",
            "  cluster watch=CONVERGED",
            "  feed CLOSED");

    private static final List<String> B10_DEMAND = List.of(
            "-- nobody has said what they want",
            "  orders FLOWING backlog=100",
            "  trades FLOWING backlog=20",
            "-- the screen folds everything away",
            "  the source is told: orders COARSE",
            "  the source is told: trades COARSE",
            "  orders FLOWING backlog=-",
            "  trades FLOWING backlog=-",
            "-- the source answers again; the backlog is not written",
            "  orders FLOWING backlog=-",
            "  trades STALLED backlog=-",
            "-- the screen opens orders",
            "  the source is told: orders FINE",
            "  orders FLOWING backlog=120",
            "  trades STALLED backlog=-",
            "-- a wall screen wants everything",
            "  the source is told: trades FINE",
            "  orders FLOWING backlog=125",
            "  trades STALLED backlog=35",
            "-- the wall screen is switched off",
            "  the source is told: trades COARSE",
            "  orders FLOWING backlog=125",
            "  trades STALLED backlog=-");

    @ParameterizedTest(name = "{0}")
    @MethodSource("steps")
    void shouldSayWhatItDid(final String step,
                            final Consumer<Consumer<String>> run,
                            final List<String> expected) {
        final List<String> told = Collections.synchronizedList(new ArrayList<>());
        run.accept(told::add);

        assertEquals(expected, told);
    }

    static Stream<Arguments> steps() {
        return Stream.of(
                step("B01Objects", B01Objects::run, B01_OBJECTS),
                step("B02Views", B02Views::run, B02_VIEWS),
                step("B03Tasks", B03Tasks::run, B03_TASKS),
                step("B04Contain", B04Contain::run, B04_CONTAIN),
                step("B05Identity", B05Identity::run, B05_IDENTITY),
                step("B06Addresses", B06Addresses::run, B06_ADDRESSES),
                step("B07Derive", B07Derive::run, B07_DERIVE),
                step("B08OwnKeys", B08OwnKeys::run, B08_OWN_KEYS),
                step("B09Feed", B09Feed::run, B09_FEED),
                step("B10Demand", B10Demand::run, B10_DEMAND));
    }

    private static Arguments step(final String name,
                                  final Consumer<Consumer<String>> run,
                                  final List<String> lines) {
        return Arguments.of(name, run, lines);
    }
}
