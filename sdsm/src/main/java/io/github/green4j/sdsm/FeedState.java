package io.github.green4j.sdsm;

/**
 * Where a feed is. It converges once its source has said an area is complete, and goes stale
 * when the source says it cannot see any more - which stops sweeping, so nothing modelled
 * disappears merely because nobody is looking at it.
 */
public enum FeedState {
    LOADING,
    CONVERGED,
    STALE,
    CLOSED
}
