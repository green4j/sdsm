package io.github.green4j.sdsm;

import java.util.Arrays;
import java.util.Objects;

/**
 * Where an object is on one axis: the parents from a root of the axis down to it, one segment
 * each. A segment names its object by {@code name}, by {@code type:name} where objects of
 * different types share a name, or by {@code #id}; the forms mix, as in
 * {@code /region:eu-de/#57/ingest-0}. A segment {@code *} or {@code type:*} stands for any one
 * object, of that type if one is given. A {@code /}, {@code :}, {@code #}, {@code *} or
 * {@code \} inside a type or a name is written after a {@code \}.
 * <p>
 * Names outlive a run and ids do not, so a path by typed names is what to keep - in a bookmark,
 * a link, an external id - and a path by ids is what to use within the run.
 */
public final class Path {

    private static final char SEPARATOR = '/';
    private static final char TYPED = ':';
    private static final char BY_ID = '#';
    private static final char ESCAPE = '\\';
    private static final char ANY = '*';

    private static final Path ROOT = new Path(new String[0], new String[0], new long[0]);

    private final String[] types;
    private final String[] names;
    private final long[] ids;

    private Path(final String[] types, final String[] names, final long[] ids) {
        this.types = types;
        this.names = names;
        this.ids = ids;
    }

    /**
     * @return the path of no segments, above every root of an axis
     */
    public static Path root() {
        return ROOT;
    }

    /**
     * @param text segments after {@code /}; the leading one may be left out
     * @return the path it spells
     */
    public static Path parse(final CharSequence text) {
        Path path = ROOT;
        final StringBuilder type = new StringBuilder();
        final StringBuilder name = new StringBuilder();
        int at = text.length() > 0 && text.charAt(0) == SEPARATOR ? 1 : 0;
        if (at == text.length()) {
            return path;
        }
        while (at <= text.length()) {
            type.setLength(0);
            name.setLength(0);
            boolean typed = false;
            boolean literal = false;
            final boolean byId = at < text.length() && text.charAt(at) == BY_ID;
            int i = byId ? at + 1 : at;
            for (; i < text.length() && text.charAt(i) != SEPARATOR; i++) {
                final char c = text.charAt(i);
                if (c == ESCAPE) {
                    literal = true;
                    i++;
                    if (i == text.length()) {
                        throw new IllegalArgumentException("Nothing after '\\' at the end of: " + text);
                    }
                    name.append(text.charAt(i));
                } else if (c == TYPED && !typed && !byId) {
                    typed = true;
                    literal = false;
                    type.append(name);
                    name.setLength(0);
                } else {
                    name.append(c);
                }
            }
            if (name.length() == 0 || typed && type.length() == 0) {
                throw new IllegalArgumentException("An empty segment at " + at + " of: " + text);
            }
            if (byId) {
                path = path.child(idOf(name, text));
            } else if (!literal && name.length() == 1 && name.charAt(0) == ANY) {
                path = path.grown(typed ? type.toString() : null, null, -1L);
            } else {
                path = path.child(typed ? type.toString() : null, name.toString());
            }
            at = i + 1;
        }
        return path;
    }

    /**
     * @param type the child's type, or null if its name is enough
     * @param name the child's name
     * @return this path and the child
     */
    public Path child(final String type, final String name) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("A segment needs a name");
        }
        return grown(type, name, -1L);
    }

    /**
     * @param id the child's id
     * @return this path and the child
     */
    public Path child(final long id) {
        if (id < 0) {
            throw new IllegalArgumentException("An id is not negative: " + id);
        }
        return grown(null, null, id);
    }

    public int length() {
        return names.length;
    }

    /**
     * @param index below {@link #length()}
     * @return the segment's type, or null if it names its object by name alone or by id
     */
    public String typeAt(final int index) {
        return types[index];
    }

    /**
     * @param index below {@link #length()}
     * @return whether the segment stands for any object, of its type if it has one
     */
    public boolean isAnyAt(final int index) {
        return names[index] == null && ids[index] < 0;
    }

    /**
     * @param index below {@link #length()}
     * @return the segment's name, or null if it names its object by id or stands for any
     */
    public String nameAt(final int index) {
        return names[index];
    }

    /**
     * @param index below {@link #length()}
     * @return the segment's id, or -1 if it names its object by name
     */
    public long idAt(final int index) {
        return ids[index];
    }

    /**
     * Writes one segment, so a path can be spelled - an external id, say - without making one.
     *
     * @param out  where to write
     * @param type the type, or null
     * @param name the name
     * @return out
     */
    public static StringBuilder appendSegment(final StringBuilder out,
                                              final CharSequence type,
                                              final CharSequence name) {
        out.append(SEPARATOR);
        if (type != null) {
            escaped(out, type).append(TYPED);
        }
        return escaped(out, name);
    }

    @Override
    public String toString() {
        if (names.length == 0) {
            return String.valueOf(SEPARATOR);
        }
        final StringBuilder out = new StringBuilder();
        for (int i = 0; i < names.length; i++) {
            if (isAnyAt(i)) {
                out.append(SEPARATOR);
                if (types[i] != null) {
                    escaped(out, types[i]).append(TYPED);
                }
                out.append(ANY);
            } else if (names[i] == null) {
                out.append(SEPARATOR).append(BY_ID).append(ids[i]);
            } else {
                appendSegment(out, types[i], names[i]);
            }
        }
        return out.toString();
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof Path)) {
            return false;
        }
        final Path path = (Path) other;
        return Arrays.equals(types, path.types) && Arrays.equals(names, path.names) && Arrays.equals(ids, path.ids);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(types), Arrays.hashCode(names), Arrays.hashCode(ids));
    }

    /**
     * @param index  below {@link #length()}
     * @param object an object
     * @return whether the segment names the object
     */
    boolean matchesAt(final int index, final StructureObject object) {
        if (ids[index] >= 0) {
            return object.id() == ids[index];
        }
        return (types[index] == null || types[index].equals(object.type()))
                && (names[index] == null || names[index].equals(object.name()));
    }

    private Path grown(final String type, final String name, final long id) {
        final int length = names.length;
        final String[] grownTypes = Arrays.copyOf(types, length + 1);
        final String[] grownNames = Arrays.copyOf(names, length + 1);
        final long[] grownIds = Arrays.copyOf(ids, length + 1);
        grownTypes[length] = type;
        grownNames[length] = name;
        grownIds[length] = id;
        return new Path(grownTypes, grownNames, grownIds);
    }

    private static long idOf(final CharSequence digits, final CharSequence text) {
        try {
            return Long.parseLong(digits, 0, digits.length(), 10);
        } catch (final NumberFormatException notAnId) {
            throw new IllegalArgumentException("Not an id: #" + digits + " in: " + text, notAnId);
        }
    }

    private static StringBuilder escaped(final StringBuilder out, final CharSequence text) {
        for (int i = 0; i < text.length(); i++) {
            final char c = text.charAt(i);
            if (c == SEPARATOR || c == TYPED || c == BY_ID || c == ANY || c == ESCAPE) {
                out.append(ESCAPE);
            }
            out.append(c);
        }
        return out;
    }
}
