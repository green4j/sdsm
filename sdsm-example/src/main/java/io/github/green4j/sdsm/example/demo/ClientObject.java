package io.github.green4j.sdsm.example.demo;

import io.github.green4j.sdsm.ObjectKind;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * One object of a {@link ClientView}, as the batches describe it. What an object is - its
 * {@code $name}, {@code $type}, the {@code $node} a port sits on - arrives under the same keys as
 * everything else, so it is one map, and nothing of it is read from the structure.
 */
public final class ClientObject {

    private final long id;
    private final ObjectKind kind;
    private final Map<String, Object> properties = new HashMap<>();
    private final Set<Long> groups = new LinkedHashSet<>();
    private final Set<Long> members = new LinkedHashSet<>();

    ClientObject(final long id, final ObjectKind kind) {
        this.id = id;
        this.kind = kind;
    }

    public long id() {
        return id;
    }

    public ObjectKind kind() {
        return kind;
    }

    public String name() {
        return String.valueOf(properties.getOrDefault("$name", ""));
    }

    public String type() {
        return String.valueOf(properties.getOrDefault("$type", ""));
    }

    public String externalId() {
        return textOf("$externalId");
    }

    public String axis() {
        return textOf("$axis");
    }

    /**
     * @return the node a port sits on, -1 if this is not a port
     */
    public long node() {
        return has("$node") ? longOf("$node") : -1L;
    }

    public boolean has(final String key) {
        return properties.containsKey(key);
    }

    public long longOf(final String key) {
        final Object value = properties.get(key);
        return value instanceof Number ? ((Number) value).longValue() : 0L;
    }

    public String textOf(final String key) {
        final Object value = properties.get(key);
        return value instanceof String ? (String) value : null;
    }

    /**
     * @return the groups it is in
     */
    public Set<Long> groups() {
        return Collections.unmodifiableSet(groups);
    }

    /**
     * @return what it holds
     */
    public Set<Long> members() {
        return Collections.unmodifiableSet(members);
    }

    /**
     * @param key   the property
     * @param value what it now holds, null for nothing
     */
    void set(final String key, final Object value) {
        if (value == null) {
            properties.remove(key);
        } else {
            properties.put(key, value);
        }
    }

    void joined(final long groupId) {
        groups.add(groupId);
    }

    void left(final long groupId) {
        groups.remove(groupId);
    }

    void gained(final long memberId) {
        members.add(memberId);
    }

    void lost(final long memberId) {
        members.remove(memberId);
    }
}
