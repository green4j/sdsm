package io.github.green4j.sdsm.example.basic;

import io.github.green4j.sdsm.ChangeCursor;
import io.github.green4j.sdsm.ChangeKind;
import io.github.green4j.sdsm.Observation;
import io.github.green4j.sdsm.StructureBatch;

import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/**
 * What the examples share: a batch written out a line per record, the properties of one
 * object gathered on one line, a wait for something another thread does, and what a source says
 * of what the structure would not take.
 */
public final class Show {

    /**
     * Writes a batch out. A batch is lent only for the call, so it is read here and nowhere
     * else - which is what any subscriber has to do.
     *
     * @param label what delivered it
     * @param batch the batch
     * @param out   where the lines go
     */
    static void batch(final String label, final StructureBatch batch, final Consumer<String> out) {
        out.accept(label + (batch.isInitialSnapshot() ? " snapshot" : " batch"));
        final StringBuilder properties = new StringBuilder();
        long propertiesOf = -1L;
        final ChangeCursor cursor = batch.cursor();
        while (cursor.next()) {
            final ChangeKind kind = cursor.changeKind();
            if (kind == ChangeKind.PROPERTY_CHANGED && cursor.objectId() == propertiesOf) {
                properties.append(' ').append(cursor.propertyKey()).append('=')
                        .append(valueOf(cursor));
                continue;
            }
            flush(properties, out);
            propertiesOf = -1L;
            if (kind == ChangeKind.PROPERTY_CHANGED) {
                propertiesOf = cursor.objectId();
                properties.append("  ").append(propertiesOf).append(':').append(' ')
                        .append(cursor.propertyKey()).append('=').append(valueOf(cursor));
            } else if (kind == ChangeKind.CONTAINED || kind == ChangeKind.UNCONTAINED) {
                out.accept("  " + kind + " " + cursor.objectId() + " " + cursor.parentId());
            } else {
                out.accept("  " + kind + " " + cursor.objectKind() + " " + cursor.objectId());
            }
        }
        flush(properties, out);
    }

    /**
     * @param condition what another thread is to bring about
     */
    public static void await(final BooleanSupplier condition) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("It did not happen in time");
            }
            Thread.yield();
        }
    }

    /**
     * What a source of an example does with what the structure would not take: says it.
     *
     * @param observation what was not taken, or null when it was not an observation
     * @param reason      why
     */
    public static void rejected(final Observation observation, final Throwable reason) {
        System.err.println("not materialized: " + (observation != null ? observation.externalId() : "what was said"));
        reason.printStackTrace(System.err);
    }

    private static void flush(final StringBuilder properties, final Consumer<String> out) {
        if (properties.length() > 0) {
            out.accept(properties.toString());
            properties.setLength(0);
        }
    }

    private static String valueOf(final ChangeCursor cursor) {
        switch (cursor.valueType()) {
            case LONG:
                return Long.toString(cursor.longValue());
            case DOUBLE:
                return Double.toString(cursor.doubleValue());
            case BOOLEAN:
                return Boolean.toString(cursor.booleanValue());
            case TEXT:
                return cursor.textValue().toString();
            default:
                return "-";
        }
    }

    private Show() {
    }
}
