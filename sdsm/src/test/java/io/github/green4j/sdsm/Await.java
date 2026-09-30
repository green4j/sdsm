package io.github.green4j.sdsm;

import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * Waits for what another thread does. Nothing is handed back to wait on, so the test looks
 * until the structure has got there, or fails when it has not in five seconds.
 */
final class Await {

    static void until(final BooleanSupplier condition) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                fail("The structure did not get there in time");
            }
            Thread.yield();
        }
    }

    private Await() {
    }
}
