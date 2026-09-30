package io.github.green4j.sdsm.example.newa;

import io.github.green4j.jelly.ByteArray;
import io.github.green4j.jelly.JsonGenerator;
import io.github.green4j.jelly.Utf8ByteArrayWriter;
import io.github.green4j.newa.json.ByteArrayJsonGenerator;
import io.github.green4j.sdsm.ChangeCursor;
import io.github.green4j.sdsm.StructureBatch;

/**
 * A batch as a frame. The records are written out as they are read, so nothing is built up
 * beside the buffer being written: a batch of a thousand changes costs the bytes of the frame
 * and nothing else.
 * <p>
 * One of these renders one view's frames. A view hands its batches over one at a time - the
 * delivery ring has a single reader - so the generator inside is never in two hands at once,
 * and the frame it produced is sent to every subscriber rather than rendered per session.
 */
public final class BatchJson {

    private static final int INITIAL_FRAME_BYTES = 4096;

    private final ByteArrayJsonGenerator json =
            new ByteArrayJsonGenerator(new Utf8ByteArrayWriter(INITIAL_FRAME_BYTES));
    private final String view;

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
        while (cursor.next()) {
            change(out, cursor);
        }
        out.endArray();
        out.endObject();
        out.eoj();
        return json.finish();
    }

    private static void change(final JsonGenerator out, final ChangeCursor cursor) {
        out.startObject();
        out.objectMember("what");
        out.stringValue(cursor.changeKind().name());
        out.objectMember("id");
        out.numberValue(cursor.objectId());
        switch (cursor.changeKind()) {
            case CONTAINED:
            case UNCONTAINED:
                out.objectMember("parent");
                out.numberValue(cursor.parentId());
                break;
            case PROPERTY_CHANGED:
                out.objectMember("key");
                out.stringValue(cursor.propertyKey(), true);
                value(out, cursor);
                break;
            default:
                out.objectMember("kind");
                out.stringValue(cursor.objectKind().name());
                break;
        }
        out.endObject();
    }

    /**
     * A value goes out as what it is. A property nobody supplies is not a value: it is
     * absent, and the frame says so rather than sending a zero.
     *
     * @param out    where to write
     * @param cursor what to write
     */
    private static void value(final JsonGenerator out, final ChangeCursor cursor) {
        out.objectMember("value");
        switch (cursor.valueType()) {
            case LONG:
                out.numberValue(cursor.longValue());
                break;
            case DOUBLE:
                out.stringValue(Double.toString(cursor.doubleValue()));
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
