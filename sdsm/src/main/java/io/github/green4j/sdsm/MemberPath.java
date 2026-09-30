package io.github.green4j.sdsm;

/**
 * Where a value is read, starting at a member of a group: on the member itself, on the object
 * one of its intrinsic ids names - {@code from.rate}, the port a link runs from - or on its
 * parent on an axis, {@code parent(placement).region}; {@code parent(placement)} alone is the
 * parent's id.
 */
final class MemberPath {

    private static final String PARENT_OPEN = "parent(";

    enum Hop {
        SELF(-1),
        FROM(PropertyKeys.FROM),
        TO(PropertyKeys.TO),
        NODE(PropertyKeys.NODE),
        PARENT(-1);

        /** The intrinsic id the hop follows, or -1. */
        final int keyId;

        Hop(final int keyId) {
            this.keyId = keyId;
        }
    }

    final Hop hop;
    /** The axis of a hop to a parent, or null. */
    final String axis;
    final int keyId;

    private MemberPath(final Hop hop, final String axis, final int keyId) {
        this.hop = hop;
        this.axis = axis;
        this.keyId = keyId;
    }

    /**
     * @param text {@code <key>}, {@code <from|to|node>.<key>}, {@code parent(<axis>)} or
     *             {@code parent(<axis>).<key>}
     * @param keys the naming dictionary
     * @return the path
     */
    static MemberPath parse(final String text, final PropertyKeys keys) {
        final Hop hop;
        String axis = null;
        final int dot;
        if (text.startsWith(PARENT_OPEN)) {
            final int close = text.indexOf(')');
            if (close < 0 || close < text.length() - 1 && text.charAt(close + 1) != '.') {
                throw new IllegalArgumentException("A hop to a parent is parent(<axis>): " + text);
            }
            axis = text.substring(PARENT_OPEN.length(), close).trim();
            if (axis.isEmpty()) {
                throw new IllegalArgumentException("A hop to a parent names its axis: " + text);
            }
            if (close == text.length() - 1) {
                return new MemberPath(Hop.PARENT, axis, PropertyKeys.ID);
            }
            hop = Hop.PARENT;
            dot = close + 1;
        } else {
            dot = text.indexOf('.');
            if (dot < 0) {
                return new MemberPath(Hop.SELF, null, keys.idOf(text));
            }
            hop = hopOf(text.substring(0, dot));
            if (hop == null) {
                throw new IllegalArgumentException("A path is <key>, <from|to|node>.<key> or parent(<axis>)"
                        + " and a key: " + text);
            }
        }
        if (text.indexOf('.', dot + 1) >= 0 || dot == text.length() - 1) {
            throw new IllegalArgumentException("A path takes one hop and a key: " + text);
        }
        return new MemberPath(hop, axis, keys.idOf(text.substring(dot + 1)));
    }

    private static Hop hopOf(final String name) {
        switch (name) {
            case "from":
                return Hop.FROM;
            case "to":
                return Hop.TO;
            case "node":
                return Hop.NODE;
            default:
                return null;
        }
    }

    /**
     * @return whether it reads past the member, on an object an intrinsic id of it names
     */
    boolean crosses() {
        return hop.keyId >= 0;
    }

    /**
     * @return what it reads past the member, as {@link Watcher} flags
     */
    int reach() {
        switch (hop) {
            case FROM:
            case TO:
                return Watcher.PORTS;
            case NODE:
                return Watcher.OWNER;
            case PARENT:
                return Watcher.PARENT;
            default:
                return 0;
        }
    }
}
