package io.github.green4j.sdsm;

/**
 * A one-off property write from outside the structure's thread: resolve the name, step onto the
 * thread, write. Code that writes a stream of them resolves the key id once and stays on the
 * thread instead.
 */
final class Props {

    static void setText(final Structure structure,
                        final long objectId,
                        final String key,
                        final String value) {
        final int keyId = structure.propertyKeys().idOf(key);
        structure.run(() -> structure.setText(objectId, keyId, value)).join();
    }

    static void setLong(final Structure structure,
                        final long objectId,
                        final String key,
                        final long value) {
        final int keyId = structure.propertyKeys().idOf(key);
        structure.run(() -> structure.setLong(objectId, keyId, value)).join();
    }

    private Props() {
    }
}
