package io.github.green4j.sdsm;

import java.time.Duration;

/**
 * Lower bound on the interval between deliveries to a view's subscribers.
 * Changes made within the interval are coalesced into the next batch.
 */
public final class DeliveryPolicy {

    private static final DeliveryPolicy ON_CHANGE = new DeliveryPolicy(0L);

    private final long minIntervalNanos;

    private DeliveryPolicy(final long minIntervalNanos) {
        this.minIntervalNanos = minIntervalNanos;
    }

    /**
     * Delivers at the end of every task that changed anything.
     *
     * @return policy with no lower bound
     */
    public static DeliveryPolicy onChange() {
        return ON_CHANGE;
    }

    /**
     * Holds deliveries at least {@code interval} apart.
     *
     * @param interval lower bound; zero means {@link #onChange()}
     * @return policy with that lower bound
     */
    public static DeliveryPolicy minInterval(final Duration interval) {
        if (interval == null || interval.isNegative()) {
            throw new IllegalArgumentException("interval must not be null or negative");
        }
        final long nanos = interval.toNanos();
        if (nanos == 0L) {
            return ON_CHANGE;
        }
        return new DeliveryPolicy(nanos);
    }

    long minIntervalNanos() {
        return minIntervalNanos;
    }
}
