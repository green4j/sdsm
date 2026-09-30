package io.github.green4j.sdsm;

/**
 * How the structure tells whoever handed it work that the work is over - because it ran,
 * because it threw, or because it was never admitted. A caller that has nothing to wait for
 * and one piece of work outstanding at a time needs this rather than a future: a loop thread
 * hands over every round, and a future per handover is an allocation per round per thread.
 */
interface Completion {

    /**
     * @param failure what stopped the work, or null when it ran to the end
     */
    void completed(Throwable failure);
}
