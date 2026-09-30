package io.github.green4j.sdsm.example.newa;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * What a client is told went wrong is JSON whatever the words: a view name or an op it sent
 * itself, or the message of an exception, is quoted, not spliced in.
 */
class ErrorFrameTest {

    @Test
    void shouldSayWhatWentWrongInWhateverWords() {
        assertEquals(List.of("view", "top\"ology", "error", "no such view: \"x\"\\\n\u0436"),
                Json.stringsIn(ErrorFrame.of("top\"ology", "no such view: \"x\"\\\n\u0436")));
        assertEquals(List.of("error", "unreadable frame"),
                Json.stringsIn(ErrorFrame.of(null, "unreadable frame")));
    }

    @Test
    void shouldNameAFailureThatCarriesNoMessage() {
        assertEquals(List.of("view", "topology", "error", "java.lang.IllegalStateException"),
                Json.stringsIn(ErrorFrame.of("topology", new IllegalStateException())));
    }
}
