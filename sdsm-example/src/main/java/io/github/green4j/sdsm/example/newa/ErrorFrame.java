package io.github.green4j.sdsm.example.newa;

import io.github.green4j.jelly.ByteArray;
import io.github.green4j.jelly.JsonGenerator;
import io.github.green4j.jelly.Utf8ByteArrayWriter;
import io.github.green4j.newa.json.ByteArrayJsonGenerator;

import java.nio.charset.StandardCharsets;

/**
 * What a client is told went wrong: {@code {"view":...,"error":...}}. The words may be the
 * client's own or an exception's, so they are quoted, never spliced in.
 */
final class ErrorFrame {

    private static final int INITIAL_FRAME_BYTES = 128;

    /**
     * @param view  the view it is about, or null
     * @param error what went wrong
     * @return the frame
     */
    static String of(final String view, final CharSequence error) {
        final ByteArrayJsonGenerator json =
                new ByteArrayJsonGenerator(new Utf8ByteArrayWriter(INITIAL_FRAME_BYTES));
        final JsonGenerator out = json.start();
        out.startObject();
        if (view != null) {
            out.objectMember("view");
            out.stringValue(view, true);
        }
        out.objectMember("error");
        out.stringValue(error, true);
        out.endObject();
        out.eoj();
        final ByteArray frame = json.finish();
        return new String(frame.array(), frame.start(), frame.length(), StandardCharsets.UTF_8);
    }

    /**
     * @param view    the view it is about, or null
     * @param failure what went wrong
     * @return the frame, saying the failure's message, or what it is when it has none
     */
    static String of(final String view, final Throwable failure) {
        final String message = failure.getMessage();
        return of(view, message != null ? message : failure.getClass().getName());
    }

    private ErrorFrame() {
    }
}
