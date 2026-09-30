package io.github.green4j.sdsm;

public interface BatchSubscriber {
    void onBatch(StructureBatch batch);

    default void onError(Throwable failure) {
    }
}