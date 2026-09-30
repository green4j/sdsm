package io.github.green4j.sdsm.example.newa;

import io.github.green4j.jelly.JsonParser;
import io.github.green4j.jelly.JsonParserListenerAdapter;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Reads a frame the way a client would, so a test sees what the client is told rather than
 * how it is spelled.
 */
final class Json {

    /**
     * @param frame a frame
     * @return the names and string values in it, in order; fails the test if it is not JSON
     */
    static List<String> stringsIn(final String frame) {
        final List<String> strings = new ArrayList<>();
        final JsonParser parser = new JsonParser();
        parser.setListener(new JsonParserListenerAdapter() {
            @Override
            public boolean onObjectMember(final CharSequence name) {
                strings.add(name.toString());
                return true;
            }

            @Override
            public boolean onStringValue(final CharSequence data) {
                strings.add(data.toString());
                return true;
            }
        });
        parser.parseAndEoj(frame);
        assertFalse(parser.hasError(), "not JSON: " + frame);
        return strings;
    }

    private Json() {
    }
}
