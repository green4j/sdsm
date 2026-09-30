# SDSM - Structural Data State Modelling framework

A Java library for keeping a live graph of named, related objects - nodes, the ports on them, the links between ports, nodes holding nodes - and telling subscribers how it changes. A structure is changed by tasks on a thread of its own; subscribers look at it through views, each a selector with a delivery policy, and receive a snapshot and then batches of changes. Change is the first-class thing, not the state.

## Modules

`sdsm` is the framework, published as `io.github.green4j:sdsm`: structure, views, delivery,
assembly, demand; JDK standard library only. `sdsm-example` holds the examples and is not
published - see [Example](#example). Java 11 or newer; `./gradlew build` builds and tests
everything.

## Getting Started

```java
try (StructureRuntime runtime = StructureRuntime.create(2, 1, "demo")) {
    Structure s = runtime.newStructure();

    // The structure is changed inside a task; creation methods return the new object.
    Node svcA = s.submit(() -> {
        Node a = s.createNode("Service A", "service");
        Node b = s.createNode("Service B", "service");
        Output out = s.addOutput(a.id(), "out", "tcp");
        Input in = s.addInput(b.id(), "in", "tcp");
        s.createLink("edge", "tcp", out.id(), in.id());
        return a;
    }).join();

    View services = s.createView("services",
            "node[type=service], between(node[type=service])",
            null,
            DeliveryPolicy.minInterval(Duration.ofMillis(50))).join();

    s.subscribe(services.id(), batch -> {
        ChangeCursor cursor = batch.cursor();
        while (cursor.next()) {
            if (cursor.valueType() == ValueType.TEXT) {
                System.out.println(cursor.objectId() + "." + cursor.propertyKey()
                        + " = " + cursor.textValue());
            }
        }
    }).join();

    int status = s.propertyKeys().idOf("status");
    s.run(() -> s.setText(svcA.id(), status, "READY")).join();
    s.flushAll().join();    // hand over what the delivery interval still holds
}
```

## Model

Three kinds of object, two relations, four mechanisms.

```
Object   = Node | Port | Link          each: id, type, name, typed properties; a node or port: externalId?
Port     = (node, side in | out, declaration?)   declaration = (address, provide | require, domain)
Link     = (out port, in port)
Contain  = (axis, parent node, child node)
```

The laws the structure keeps by itself:

1. **A port belongs to one node, a link joins an out port to an in port.** A node takes its ports with it, a port its links.
2. **Every axis is a forest.** An axis - `placement`, `stage`, `colour` - is what `contain(parent, child, axis)` builds: a node has at most one parent per axis, holds children on one axis only, and nothing is under itself, even across axes. One node stands on several axes, so a client lays the structure out by choosing one, with no second request.
3. **A link follows from declarations.** It is there exactly while one port provides and the other requires the same address, on opposite sides, in domains that meet: the same one, a provider in none, or an open route. Its type is the provider's.
4. **A path is a node's ancestors on an axis.** Every node has one; two may share a path by names, and then a segment takes a type or an id.
5. **A property is said or derived, never both.** A derived one is one fold - `derive(object, key, fold, Over)` - over the object's own properties, the children on its axis, both, or the members of a group a grouping made, and the structure keeps it up to date. A fold reads numbers, not units: a key is one quantity in one unit across the structure, and `SUM` is for what adds up - a rate, a backlog - while a lag or a share takes `MAX` or `MIN`.
6. **An object lives while its source says it.** It belongs to a (source, scope); what the scope does not repeat by `complete(scope)` goes, and repeating a version costs nothing.
7. **Clients look through sets.** A view is a selector, keys and a delivery policy; its membership follows properties, addresses and placement as they change. A view watches the world and is not part of it.
8. **Only what is looked at is paid for.** Interest is a detail level per object, the highest any subscriber asks for; a property a source supplies from level L is written and asked for only while someone wants L.

What is left out on purpose: groups as a kind of their own (a container is a node with children, a set is a selector); relations other than containment and links (a stage, a family, a channel is a property); and either end of a link naming the other (the address does, so sources arrive in any order).

A set cut by a key is a node per key the structure keeps - `groupBy(name, type, selector, by...)`, its folds declared with `deriveEach(grouping, key, fold, Over.members(path))`: the flow links between the same two nodes carrying the same family are one bundle with the sum of their rates. A path is a key of the member, or one hop and a key: `from.`, `to.` and `node.` through an intrinsic id, `parent(axis)` to the node holding the member on an axis (`parent(placement)` alone is its id), so `groupBy("bands", "band", "node[layer=logical]", "parent(placement)", "stage")` is a band per stage within each silo. A group's external id is `<name>:<key>`, the key's parts joined by `,` and a text that could be taken for a number or a flag quoted - `bands:5` is not `bands:"5"` - and while the grouping lives no other object takes an id under its name.

## Object Identity

- **`id`** - a `long` the structure assigns at creation, unique within it, stable for the life of the object and opaque: the handle every other call takes.

- **`externalId`** - the id the *modelled thing* carries in the world that owns it: a pod's UID, a queue's name. It identifies the original, not the object standing for it, so an object holds it for life and no second object may take it. It is optional: a node the structure drew for an axis of its own models nothing outside and has none. Given when the object is made (`createNode(name, type, externalId)` and its siblings), resolved back through `findByExternalId(externalId)` on the structure's thread, so observing the same thing again reaches the object already standing for it instead of making a second one. It can be selected on.

  Uniqueness is demanded across the whole structure while an id is only unique in its own world, so a structure mirroring two worlds needs the world folded into the id (`k8s:pod:<uid>`) by whoever writes it. The structure stores and compares the id and never reads anything out of it.

An object has no single position to name, since it is placed along several axes at once; it has one per axis.

- **`Path`** - where an object is on one axis: its ancestors from a root of the axis down to it. A segment is `name`, `type:name` where objects of different types share a name, `#id`, or `*` / `type:*` for any one; the forms mix: `/region:eu-de/#57/ingest-0`. `/ : # * \` inside a name are escaped with `\`. `resolve(axis, Path.parse(text))` gives the id, or -1, and fails if a segment by name alone matches more than one object; `pathOf(id, axis)` gives the path by typed names, which `resolve` takes back to the same object. Names outlive a run and ids do not, so a path by typed names is what to keep in a bookmark, a link or an external id.

## Addresses

A connection is often not observable. A pod's configuration says which queue it reads and which it writes; the two ends of that queue are seen apart, out of order, and sometimes far apart in time. So neither end names the other. A port declares an address instead - `provide(portId, address)` when the thing it stands for is reachable there, `require(portId, address)` when it is looking for it - and the structure draws the link as soon as both declarations exist, in whichever order they arrive. The link goes when either declaration does, is `isDerived()`, and cannot be removed or rewired on its own: it belongs to the declarations, not to the caller.

An address is one opaque text. Matching is equality, so the structure never needs its parts: scheme, host, path and whatever else are the caller's business, spelled into the text handed over. It is an intrinsic property of the port, so a selector finds who is waiting for what: `input[address="tb:trades.eu"]`.

A link runs from an output to an input, and that is the whole direction rule: a providing input takes links from requiring outputs, a providing output feeds requiring inputs. A queue is therefore a node of its own, both of whose ports provide the same address - which is what lets `in-rate != out-rate` stand for a backlog, something a single link could not carry.

### Domains

Blue and green run the same pipeline and publish under the same names, so the address alone cannot say which copy of the world is meant. A declaration names a domain - an `int` interned by `structure.domains()`, the same way property names are - and a requirer reaches a provider in its own domain, or one that belongs to no domain at all: the shared queue both colours read from. Reaching another copy takes a route, `allowRoute(requiringDomain, providingDomain)`, because which copy answers is the provider's to decide and not the requirer's; `denyRoute(...)` closes it and takes the links it allowed with it. A structure that mirrors one world names no domain and never meets one.

## Assembly

Assembly is a layer above the structure: it resolves identity, compares versions and sweeps, and its only way in is the public API a caller has. A source pushes what it sees into a `Feed`; the feed keeps the last observation per id and hands the lot to the structure as one task, which is one transaction. The feeds of a `LoopGroup` run on its loops, one or more to a thread. The loop writes nothing and waits for nothing, so a source that has gone quiet - or a structure that is busy - holds up no other loop. While a batch is in flight the loop goes on coalescing, so a source talking faster than the structure writes costs batches, not queue.

An observation carries the id the thing has in its own world, the area it belongs to, and whatever that world calls this state of it. The version is compared, never read into: an observation repeating a version already materialized costs a lookup and nothing else. The area is the source's own number for a part of the world it can speak for as a whole - when it says `complete(area)`, whatever it had put there and has not mentioned this time round is gone. A source that reports its removals instead says `removed(externalId)` and is never swept.

`Materializer` runs on the structure's thread, inside that transaction, and writes through `Emit` - a class, not an interface anyone can be handed: the structure is not in its reach, so it cannot start a task, wait on one, or read around what it is writing. Every object it names, it names by the id the observed world knows it by, so materializing twice reaches what the first time made.

An object stays as long as something says it is there. Two clusters putting themselves in the same region both say the region is there, and it outlives the first of them to stop - sweeping takes away what nothing claims any more, not what one thing stopped claiming.

A feed is `LOADING` until its source completes an area, then `CONVERGED`. `unavailable(reason)` makes it `STALE` and stops sweeping: a thing nobody can see is not a thing that has gone. The materializer is told, so a binding can put that in the graph - what staleness means for the things being modelled is the binding's to decide, and to the structure `state` is an ordinary property.

## Demand

Everything else here runs from the world to the screen; demand runs back. A client says what it is looking at - `structure.setInterest(client, Interest.of(COARSE).at(nodeId, FINE))` - and what reaches a source is a level per thing, on the loop thread: `detailLevelChanged(externalId, level)`. That is the only place work can actually be saved, because only the source knows what a fetch costs.

Demand is the highest level any client asks for, so a thing one window has put away stays whole while another is showing it. An interest is replaced whole rather than amended: a display that has just changed knows what it is showing and does not know what it told anyone last time. An object no client has named is wanted at that client's base level, which is what a thing arriving under a collapsed group gets until the client speaks again.

A source declares what it writes that is not worth writing at every level - `suppliedProperties()` - and a property named there is written only while the thing it belongs to is wanted at that level or higher. When demand falls, what the source would no longer send is removed at once rather than left standing: a value nobody is paying for is not a value anyone should be shown. That is also why a predicate never matches what is unknown, either way round - `cpu!=0` holds no object whose `cpu` is not supplied, rather than holding all of them.

Collapsing and expanding stay in the display and reach the framework as nothing but a new interest; what a source does with a level is its own business, and one that pays nothing per observation may ignore it.

Being unwanted is not being gone. A thing the framework told a source to stop looking at is not swept for not having been seen, so what a display puts away is still there to expand back to.

## Architecture Decisions

### 1. Single-writer per structure, multi-reader runtime

Every `Structure` is bound to one worker thread of its `StructureRuntime`, and its state is touched there only: no locks inside the model, and one total order of changes. Structures of one runtime are independent and progress in parallel - concurrency is between structures, not inside one.

### 2. Change-centric model with batching

SDSM delivers **batches**, not a callback per change, and a batch is internally consistent. A write that changes nothing is dropped: a source that says the whole of its area every round writes mostly what is already there. A view owes a property, not a history of one: the value delivered is the one the structure holds when the batch is built, which is also what a snapshot of the view would say.

A batch carries the shape of the graph, not only its values. A relation an object has exactly one of is an intrinsic property of that object - a port answers which node it is on, a link which ports it joins - and containment, the one relation a node can have several of (a parent per axis), arrives as `CONTAINED` / `UNCONTAINED` records naming the child and the parent. A receiver can therefore build and keep a copy of what a view holds out of the batches alone, with no access to the structure and no dictionary of its own: a record carries the name of its key. A snapshot says what is there before it says anything about it, so a record naming two objects is never read before both have arrived.

### 3. Views as first-class entities

A view is named and long-lived, with a selector, keys, a delivery policy and a membership of its own. It stands outside the structure: no selector finds it, and it is made, changed and removed through `createView`, `setViewSelector` and `removeView`. A subscriber attaching to it gets a snapshot of what it holds, then deltas.

### 4. Delivery decoupled from change detection

A view's `DeliveryPolicy` is a lower bound on the interval between deliveries: `onChange()` delivers at the end of every task that changed anything, `minInterval(duration)` holds deliveries that far apart and coalesces whatever happens in between.

### 5. Property writes are typed, keyed by int, and cost nothing to carry

A property name is interned once into `PropertyKeys`, one dictionary for the process, so a key id means the same name in every structure; everything downstream - the typed setters, the compiled selectors, the change records - carries the `int`. Values are stored and delivered as `long` / `double` / `boolean` / text, never boxed; a `ValueType` says which, or `ABSENT` for a value removed. A batch is a flat `long[]` of fixed-width records plus a `char[]` for text, read through a `ChangeCursor`, and it comes back to the engine empty to be filled again.

A stream of typed writes resolves its key ids once; the measured result is zero bytes allocated on the structure's thread per change, folds included, however many changes pass.

### 6. Delivery runs on the application's threads

`withDeliveryExecutor(...)` hands subscriber callbacks to an executor of the caller's own; without one they run on the structure's thread and a slow subscriber holds up the model. Batches travel to the delivery thread and back through per-view queues, so a delivery executor that falls behind holds the drain back and the changes keep coalescing rather than piling up.

`snapshotView(viewId, subscriber)` re-states a view to one subscriber: what the view owes everyone goes first, then a complete snapshot built from current state. That is what a session that has fallen behind asks for. A delta can reach a session ahead of a snapshot built while it was being published, and what it said is in the snapshot; a receiver therefore takes a snapshot as the whole view and then only the deltas whose sequence number is past the snapshot's.

### 7. A task is the transaction, and it names its writer

The structure is changed only inside a task, and everything a task changes reaches subscribers as one batch - so a whole subtree can be built, wired and populated before anyone is told about any of it. Reads, views, subscriptions and interest are asked from any thread and answer with a future:

```java
s.run(KUBERNETES, () -> {
    Node pod = s.createNode(podName, "pod", podUid);
    s.setText(pod.id(), state, "Running");
}).join();
```

The future completes once the task has run. By then a view delivering `onChange()` has called its subscribers with the task's batch, or queued it on the delivery executor if that has room; a view with a `minInterval` holds it until the interval has passed or `flushAll()`. A task that fails is not rolled back: what it did before failing is delivered like anything else.

`run(sourceId, ...)` attributes every property the task writes to a source; the attribution holds for whatever the task calls and is restored when it returns. The id is the caller's own number - the structure stores it, compares it and never interprets it - and `sourceOf(keyId)` reads it back. Two sources can write different properties of one object and each can still tell what it is answerable for.

### 8. Selector language as a small DSL

A selector is a kind, `[key op value]` predicates (`=`, `!=`, `~` for a substring, `<` `<=` `>` `>=` between numbers), `under(axis, path)`, `between(e)` and `touching(e)` for links by their ends' nodes, `,` for a union, `&` for an intersection and parentheses: `node[type=service], between(node[type=service])`. It is kept up to date as properties, placement and ends change, and stops short of a query engine on purpose. The grammar is in [`sdsm/README.md`](sdsm/README.md).

### 9. Reserved properties and stable identity

`id`, `kind`, `name`, `type`, `externalId`, `axis`, `address`, `node`, `from`, `to`, `role`, `domain` and `links` hold the first key ids and are answered from the object's own fields rather than from its property store - one namespace, so a selector and a change record treat them like any other key. They cannot be set or removed through the typed setters, nor derived or folded on an object or over its children. `id`, `kind`, `name`, `type` and `externalId` never change, so they can be read from any thread, and `axis` is fixed once a node holds something; `address` is mutable through `provide(...)` / `require(...)` / `clearAddress(...)`, which also redraw the links it implies. `node` is what a port sits on, `role` (`provide` / `require`) and `domain` come with its address, `links` counts its links; `from` and `to` are what a link joins; a view that filters property keys still delivers all of these, because what an object is is not what it reports.

### 10. Nothing is persisted

A structure is a mirror of worlds that keep their own state, so a restart asks the sources again rather than loading a copy that may have gone stale.

## When SDSM is *not* the right tool

- You need a **distributed**, multi-master graph store - SDSM is in-process and single-writer per structure.
- You need **complex graph queries** (paths, pattern matching, transitive closures beyond simple containment) - use a proper graph database.
- You need **historical / temporal queries** over past states - SDSM emits change batches but does not retain history beyond the current state.

## Example

`sdsm-example` is not part of the framework. Its `basic` package is a ladder, one topic a
step, each using only what the steps before it showed; `./gradlew :sdsm-example:runBasic
-Pstep=<n>` runs one and prints what it did.

| step | shows |
|---|---|
| `B01Objects` | nodes, ports, links, typed properties, queries, cascading removal |
| `B02Views` | selectors, a snapshot then deltas, key filters, links by their ends, changing a selector |
| `B03Tasks` | a task as one batch, writers named per task, failed tasks, held deliveries, re-snapshots |
| `B04Contain` | nodes holding nodes, axes, containment records, what is refused |
| `B05Identity` | external ids: given at creation, found again, one object at a time |
| `B06Addresses` | links drawn where addresses meet, domains, routes opened and closed |
| `B07Derive` | a node's sum and maximum over its children, nested, with how many are known |
| `B08OwnKeys` | one value out of what two sources each say about the same thing: a fold over an object's own properties |
| `B09Feed` | a source and a materializer: versions, sweeping a complete round, a blind source |
| `B10Demand` | interests per client, levels, what a source is told and what is dropped |

The `demo` package puts the steps together: a small web shop that lives on its own - load
wanders, a service falls over and comes back, one is scaled out, one is deployed and one taken
away - watched by one source and drawn on a terminal, `./gradlew :sdsm-example:run`. Services
never name each other: each says what it is reachable at and what it calls, and the structure
joins them; a tier sums its services' requests and counts those that are down. The screen is
drawn from `ClientView`, what the view holds as the client holds it, built out of the delivered
batches alone, so what is on it is what a browser at the other end of a socket would have been
able to draw.

The `newa` package serves the same model over a websocket rather than drawing it:
`./gradlew :sdsm-example:runServer`, then `{"op":"subscribe","view":"topology"}` brings a
snapshot and the deltas after it, and `{"op":"interest","base":"COARSE"}` takes the requests
per second off the wire, since the source says they are worth writing only at `FINE`. Delivery
runs on the server's own threads, a frame is rendered once per publication rather than once per
session, and a session that cannot keep up is skipped and re-synchronized rather than
disconnected. The join is the package itself - `ViewChannel`, `ClientFrames`, `ViewsApi` - on
[newa](https://github.com/green4j/newa); it is not published, because the protocol is a choice
and not a mechanism, and it is short enough to copy onto another server. The framework does not
know a websocket exists.

## License

MIT. See `LICENSE` in the repository root.