package io.github.green4j.sdsm.example.demo;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DemoTest {

    @Test
    void shouldShowWhatTheStructureHoldsAtEveryTick() {
        try (Demo demo = new Demo()) {
            final Screen screen = new Screen(demo.view(), false);
            for (int i = 0; i < 2 * World.CYCLE; i++) {
                demo.tick();

                assertEquals(count(demo, "node[type=service]"),
                        demo.view().ofType(Shop.SERVICE).size());
                assertEquals(count(demo, "link"), demo.view().links().size());
                for (final Map<String, Object> service
                        : demo.structure().query("node[type=service]").join()) {
                    final String name = (String) service.get("name");
                    assertEquals(service.get("status"),
                            demo.view().byExternalId(name).textOf("status"));
                    assertTrue(screen.render(demo.world().now(), null).contains(name));
                }
            }
        }
    }

    private static int count(final Demo demo, final String selector) {
        return demo.structure().matchedObjectIds(selector).join().length;
    }
}
