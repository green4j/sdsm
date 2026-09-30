package io.github.green4j.sdsm;

import java.util.HashMap;
import java.util.Map;

/**
 * Domain names, interned to {@code int} ids once for the whole process, so an id means the
 * same domain in every structure. A domain is the copy of the
 * world an address is declared in: blue and green run the same pipeline and publish under
 * the same names, and only the domain tells the two apart.
 * <p>
 * Ids are dense and never reused. Id 0 is the unnamed domain, which is where a declaration
 * lands unless the caller names one, so a structure mirroring a single world never meets a
 * domain at all.
 */
public final class Domains {

    /**
     * The domain of a caller that has not named one.
     */
    public static final int NO_DOMAIN = 0;

    static final Domains OF_PROCESS = new Domains();

    private final Map<String, Integer> idsByName = new HashMap<>();
    private String[] namesById = new String[8];
    private int count;

    private Domains() {
        namesById[0] = "";
        idsByName.put("", Integer.valueOf(NO_DOMAIN));
        count = 1;
    }

    /**
     * @param name domain name
     * @return the id of the name, assigning one if the process has not seen it before
     */
    public synchronized int idOf(final String name) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("domain name must not be null or blank");
        }
        final Integer existing = idsByName.get(name);
        if (existing != null) {
            return existing.intValue();
        }
        if (count == namesById.length) {
            final String[] grown = new String[count * 2];
            System.arraycopy(namesById, 0, grown, 0, count);
            namesById = grown;
        }
        final int assigned = count;
        namesById[assigned] = name;
        count = assigned + 1;
        idsByName.put(name, Integer.valueOf(assigned));
        return assigned;
    }

    /**
     * @param name domain name, empty meaning the unnamed domain
     * @return the id of the name, or -1 if the process has never seen it
     */
    public synchronized int lookup(final String name) {
        final Integer existing = idsByName.get(name == null ? "" : name);
        return existing == null ? -1 : existing.intValue();
    }

    /**
     * @param domainId a domain id
     * @return the name the id stands for; empty for {@link #NO_DOMAIN}
     */
    public synchronized String nameOf(final int domainId) {
        requireKnown(domainId);
        return namesById[domainId];
    }

    /**
     * @return how many domains the process has interned, the unnamed one included
     */
    public synchronized int count() {
        return count;
    }

    synchronized void requireKnown(final int domainId) {
        if (domainId < 0 || domainId >= count) {
            throw new IllegalArgumentException("No such domain id: " + domainId);
        }
    }
}
