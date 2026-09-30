package io.github.green4j.sdsm;

/**
 * Told, on the structure's thread, that what is wanted of an object has changed. Assembly
 * listens: a feed drops what its source would no longer supply and passes the word on to the
 * source, which is where the fetching stops.
 */
interface DemandListener {

    void demandChanged(StructureObject object, DetailLevel level);
}
