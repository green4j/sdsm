package io.github.green4j.sdsm;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Property names, interned to {@code int} ids once for the whole process. The id is what the
 * typed setters, the compiled selectors and the change records carry, so a name is compared
 * once - when it is first used - and never again; and it means the same name in every
 * structure, so an id taken from one is right for another.
 * <p>
 * Ids are dense, assigned on first use and never reused, which makes a key id a stable handle
 * for the life of the process. The intrinsic keys - the ones every object answers from its
 * own fields rather than from its property store - hold the first ids. Their names, and paths
 * through them, alone start with {@link #INTRINSIC}: what the structure keeps never takes a name
 * from what it is told. A relation an object has exactly one of is among them: a port answers
 * which node it is on, a link which ports it joins, so a receiver reads the shape of the graph out
 * of the same records as everything else.
 */
public final class PropertyKeys {

    /** What an intrinsic key's name starts with, and no other key's may. */
    public static final char INTRINSIC = '$';

    public static final int ID = 0;
    public static final int NAME = 1;
    public static final int TYPE = 2;
    public static final int EXTERNAL_ID = 3;
    public static final int AXIS = 4;
    public static final int ADDRESS = 5;
    public static final int NODE = 6;
    public static final int FROM = 7;
    public static final int TO = 8;
    public static final int ROLE = 9;
    public static final int DOMAIN = 10;
    public static final int LINKS = 11;

    static final int INTRINSIC_COUNT = 12;

    private static final String[] INTRINSIC_NAMES = {"$id", "$name", "$type", "$externalId",
        "$axis", "$address", "$node", "$from", "$to", "$role", "$domain", "$links"};

    static final PropertyKeys OF_PROCESS = new PropertyKeys();

    private final Map<String, Integer> idsByName = new HashMap<>();
    // read without the lock: a name is written before the count that makes it readable
    private volatile String[] namesById = new String[16];
    private volatile int count;

    private PropertyKeys() {
        for (int i = 0; i < INTRINSIC_NAMES.length; i++) {
            idsByName.put(INTRINSIC_NAMES[i], Integer.valueOf(i));
            namesById[i] = INTRINSIC_NAMES[i];
        }
        count = INTRINSIC_NAMES.length;
    }

    /**
     * @param name property name
     * @return the id of the name, assigning one if the process has not seen it before
     */
    public synchronized int idOf(final String name) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("property name must not be null or blank");
        }
        final Integer existing = idsByName.get(name);
        if (existing != null) {
            return existing.intValue();
        }
        if (name.charAt(0) == INTRINSIC && !isHop(name)) {
            throw new IllegalArgumentException("No intrinsic key '" + name + "'; a property's name does not start with "
                    + INTRINSIC);
        }
        final int assigned = count;
        String[] names = namesById;
        if (assigned == names.length) {
            names = Arrays.copyOf(names, assigned * 2);
        }
        names[assigned] = name;
        namesById = names;
        count = assigned + 1;
        idsByName.put(name, Integer.valueOf(assigned));
        return assigned;
    }

    /**
     * @param name property name
     * @return the id of the name, or -1 if the process has never seen it
     */
    public synchronized int lookup(final String name) {
        final Integer existing = idsByName.get(name);
        return existing == null ? -1 : existing.intValue();
    }

    /**
     * @param keyId a key id
     * @return the name the id stands for
     */
    public String nameOf(final int keyId) {
        if (keyId < 0 || keyId >= count) {
            throw new IllegalArgumentException("No such property key id: " + keyId);
        }
        return namesById[keyId];
    }

    /**
     * @return how many names the process has interned
     */
    public int count() {
        return count;
    }

    /**
     * A path through an intrinsic id, {@code $from.$node}, is the structure's own name for what keys a
     * group, and the only other name that starts with {@link #INTRINSIC}.
     *
     * @param name a name that starts with it
     * @return whether it is such a path
     */
    private boolean isHop(final String name) {
        final int dot = name.indexOf('.');
        return dot > 0 && idsByName.containsKey(name.substring(0, dot));
    }

    static boolean isReserved(final int keyId) {
        return keyId < INTRINSIC_COUNT;
    }
}
