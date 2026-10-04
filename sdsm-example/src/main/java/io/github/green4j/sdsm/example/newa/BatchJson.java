package io.github.green4j.sdsm.example.newa;

import io.github.green4j.jelly.ByteArray;
import io.github.green4j.jelly.JsonGenerator;
import io.github.green4j.jelly.Utf8ByteArrayWriter;
import io.github.green4j.newa.json.ByteArrayJsonGenerator;
import io.github.green4j.sdsm.ChangeCursor;
import io.github.green4j.sdsm.ChangeKind;
import io.github.green4j.sdsm.ObjectKind;
import io.github.green4j.sdsm.PropertyKeys;
import io.github.green4j.sdsm.StructureBatch;

import java.math.BigDecimal;
import java.util.Arrays;

/**
 * A batch as a frame. The records are written out as they are read, so nothing is built up
 * beside the buffer being written: a batch of a thousand changes costs the bytes of the frame
 * and nothing else.
 * <pre>
 * {"view":"topology","seq":5,"version":3732,"snapshot":true,
 *  "changes":[["+",7,"NODE"],["p",7,1,"billing-0",2,"billing"],["c",7,3],...],
 *  "keys":{"1":"$name","2":"$type"}}
 * </pre>
 * A change is {@code ["+", id, kind]} added, {@code ["-", id, kind]} removed, {@code ["c", id,
 * parent]} contained, {@code ["u", id, parent]} uncontained, or {@code ["p", id, key, value,
 * key, value...]}: what properties of one object now are, the batch's run of them. A key is a
 * number, and {@code keys} names each one the frame uses, so every frame reads on its own.
 * {@code $id} is the change's own and not repeated as a property, and a
 * port's {@code externalId} is its node's, its side and its name ({@code Port.appendExternalId}),
 * so it is not sent either.
 * <p>
 * One of these renders one view's frames. A view hands its batches over one at a time - the
 * delivery ring has a single reader - so the generator inside is never in two hands at once,
 * and the frame it produced is sent to every subscriber rather than rendered per session.
 */
public final class BatchJson {

    private static final int INITIAL_FRAME_BYTES = 4096;
    private static final long NO_RUN = -1L;

    private final ByteArrayJsonGenerator json =
            new ByteArrayJsonGenerator(new Utf8ByteArrayWriter(INITIAL_FRAME_BYTES));
    private final String view;
    private final StringBuilder keyId = new StringBuilder(8);
    private String[] keyNames = new String[64];
    private boolean[] used = new boolean[64];
    private int[] usedIds = new int[64];
    private int usedCount;

    /**
     * @param view what to call the view in the frame
     */
    public BatchJson(final String view) {
        this.view = view;
    }

    /**
     * @param batch what to render
     * @return the frame, valid until the next call
     */
    public ByteArray render(final StructureBatch batch) {
        final JsonGenerator out = json.start();
        out.startObject();
        out.objectMember("view");
        out.stringValue(view, true);
        out.objectMember("seq");
        out.numberValue(batch.batchSequenceNumber());
        out.objectMember("version");
        out.numberValue(batch.structureVersion());
        out.objectMember("snapshot");
        if (batch.isInitialSnapshot()) {
            out.trueValue();
        } else {
            out.falseValue();
        }
        out.objectMember("changes");
        out.startArray();
        final ChangeCursor cursor = batch.cursor();
        long run = NO_RUN;
        while (cursor.next()) {
            run = change(out, cursor, run);
        }
        if (run != NO_RUN) {
            out.endArray();
        }
        out.endArray();
        keys(out);
        out.endObject();
        out.eoj();
        return json.finish();
    }

    /**
     * @param out    where to write
     * @param cursor what to write
     * @param run    the object whose properties are being written, or {@link #NO_RUN}
     * @return the object whose properties are being written after this change
     */
    private long change(final JsonGenerator out, final ChangeCursor cursor, final long run) {
        final ChangeKind kind = cursor.changeKind();
        if (kind == ChangeKind.PROPERTY_CHANGED) {
            final int key = cursor.propertyKeyId();
            if (key == PropertyKeys.ID || key == PropertyKeys.EXTERNAL_ID && isPort(cursor)) {
                return run;
            }
            if (run != cursor.objectId()) {
                if (run != NO_RUN) {
                    out.endArray();
                }
                out.startArray();
                out.stringValue("p");
                out.numberValue(cursor.objectId());
            }
            use(key, cursor);
            out.numberValue(key);
            value(out, cursor);
            return cursor.objectId();
        }
        if (run != NO_RUN) {
            out.endArray();
        }
        out.startArray();
        switch (kind) {
            case ADDED:
                out.stringValue("+");
                out.numberValue(cursor.objectId());
                out.stringValue(cursor.objectKind().name());
                break;
            case REMOVED:
                out.stringValue("-");
                out.numberValue(cursor.objectId());
                out.stringValue(cursor.objectKind().name());
                break;
            case CONTAINED:
                out.stringValue("c");
                out.numberValue(cursor.objectId());
                out.numberValue(cursor.parentId());
                break;
            default:
                out.stringValue("u");
                out.numberValue(cursor.objectId());
                out.numberValue(cursor.parentId());
                break;
        }
        out.endArray();
        return NO_RUN;
    }

    private static boolean isPort(final ChangeCursor cursor) {
        return cursor.objectKind() == ObjectKind.INPUT || cursor.objectKind() == ObjectKind.OUTPUT;
    }

    private void use(final int key, final ChangeCursor cursor) {
        if (key >= used.length) {
            final int grown = Math.max(key + 1, used.length * 2);
            used = Arrays.copyOf(used, grown);
            keyNames = Arrays.copyOf(keyNames, grown);
        }
        if (keyNames[key] == null) {
            keyNames[key] = cursor.propertyKey();   // a key's name never changes
        }
        if (!used[key]) {
            used[key] = true;
            if (usedCount == usedIds.length) {
                usedIds = Arrays.copyOf(usedIds, usedCount * 2);
            }
            usedIds[usedCount++] = key;
        }
    }

    /**
     * Names every key the frame used, and forgets them for the next one.
     *
     * @param out where to write
     */
    private void keys(final JsonGenerator out) {
        out.objectMember("keys");
        out.startObject();
        for (int i = 0; i < usedCount; i++) {
            keyId.setLength(0);
            keyId.append(usedIds[i]);
            out.objectMember(keyId);
            out.stringValue(keyNames[usedIds[i]], true);
            used[usedIds[i]] = false;
        }
        usedCount = 0;
        out.endObject();
    }

    /**
     * A value goes out as what it is: a fraction as the shortest decimal that is it, and only
     * what JSON has no number for - NaN, an infinity - as text. A property nobody supplies is
     * not a value: it is absent, and the frame says so rather than sending a zero.
     *
     * @param out    where to write
     * @param cursor what to write
     */
    private static void value(final JsonGenerator out, final ChangeCursor cursor) {
        switch (cursor.valueType()) {
            case LONG:
                out.numberValue(cursor.longValue());
                break;
            case DOUBLE:
                final double fraction = cursor.doubleValue();
                if (Double.isFinite(fraction)) {
                    final BigDecimal decimal = BigDecimal.valueOf(fraction);   // 17 digits at most: a long
                    out.numberValue(decimal.unscaledValue().longValueExact(), -decimal.scale());
                } else {
                    out.stringValue(Double.toString(fraction));
                }
                break;
            case BOOLEAN:
                if (cursor.booleanValue()) {
                    out.trueValue();
                } else {
                    out.falseValue();
                }
                break;
            case TEXT:
                out.stringValue(cursor.textValue(), true);
                break;
            default:
                out.nullValue();
                break;
        }
    }
}
