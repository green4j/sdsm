package io.github.green4j.sdsm.example.demo;

import io.github.green4j.sdsm.BatchSubscriber;
import io.github.green4j.sdsm.ChangeCursor;
import io.github.green4j.sdsm.ObjectKind;
import io.github.green4j.sdsm.StructureBatch;
import io.github.green4j.sdsm.ValueType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * What one view holds, as the client it is delivered to holds it: built out of the batches and
 * nothing else - no structure, no dictionary, no asking. A browser holding a websocket does
 * exactly this, and the demo draws from it rather than from the structure so that it can.
 * <p>
 * A snapshot replaces everything, a delta changes what it names, and a record about something
 * unknown is ignored - which is what makes it safe for a snapshot to arrive after deltas the
 * client was not there for.
 * <p>
 * Batches arrive on whichever thread delivers them and the screen is drawn on another, so
 * everything here is under one lock: the view is small, and a reader holding it for the
 * length of a frame costs nothing anyone can see.
 */
public final class ClientView implements BatchSubscriber {

    private final Map<Long, ClientObject> byId = new HashMap<>();
    private final Map<String, ClientObject> byExternalId = new HashMap<>();

    private long structureVersion;
    private int batches;
    private int snapshots;
    private int records;
    private Throwable failure;

    @Override
    public synchronized void onBatch(final StructureBatch batch) {
        if (batch.isInitialSnapshot()) {
            byId.clear();
            byExternalId.clear();
            snapshots++;
        }
        final ChangeCursor cursor = batch.cursor();
        while (cursor.next()) {
            records++;
            switch (cursor.changeKind()) {
                case ADDED:
                    added(cursor.objectId(), cursor.objectKind());
                    break;
                case REMOVED:
                    removed(cursor.objectId());
                    break;
                case CONTAINED:
                    joined(cursor.objectId(), cursor.parentId());
                    break;
                case UNCONTAINED:
                    left(cursor.objectId(), cursor.parentId());
                    break;
                case PROPERTY_CHANGED:
                    changed(cursor);
                    break;
                default:
                    break;
            }
        }
        structureVersion = batch.structureVersion();
        batches++;
    }

    @Override
    public synchronized void onError(final Throwable reason) {
        failure = reason;
    }

    public synchronized ClientObject byId(final long id) {
        return byId.get(Long.valueOf(id));
    }

    public synchronized ClientObject byExternalId(final String externalId) {
        return byExternalId.get(externalId);
    }

    /**
     * @param type what to look for, null for everything
     * @return the objects of that type, in no particular order
     */
    public synchronized List<ClientObject> ofType(final String type) {
        final List<ClientObject> found = new ArrayList<>();
        for (final ClientObject object : byId.values()) {
            if (type == null || type.equals(object.type())) {
                found.add(object);
            }
        }
        return found;
    }

    /**
     * @return the links, in no particular order
     */
    public synchronized List<ClientObject> links() {
        final List<ClientObject> found = new ArrayList<>();
        for (final ClientObject object : byId.values()) {
            if (object.kind() == ObjectKind.LINK) {
                found.add(object);
            }
        }
        return found;
    }

    /**
     * @param axis the axis
     * @return the groups on it that are in no group themselves - where a tree starts
     */
    public synchronized List<ClientObject> rootsOf(final String axis) {
        final List<ClientObject> roots = new ArrayList<>();
        for (final ClientObject object : byId.values()) {
            if (axis.equals(object.axis()) && object.groupCount() == 0) {
                roots.add(object);
            }
        }
        return roots;
    }

    public synchronized int size() {
        return byId.size();
    }

    public synchronized int batches() {
        return batches;
    }

    public synchronized int snapshots() {
        return snapshots;
    }

    public synchronized int records() {
        return records;
    }

    public synchronized long structureVersion() {
        return structureVersion;
    }

    public synchronized Throwable failure() {
        return failure;
    }

    private void added(final long id, final ObjectKind kind) {
        final Long key = Long.valueOf(id);
        if (!byId.containsKey(key)) {
            byId.put(key, new ClientObject(id, kind));
        }
    }

    private void removed(final long id) {
        final ClientObject gone = byId.remove(Long.valueOf(id));
        if (gone == null) {
            return;
        }
        for (int i = 0; i < gone.groupCount(); i++) {
            final ClientObject group = byId(gone.groupAt(i));
            if (group != null) {
                group.lost(id);
            }
        }
        for (int i = 0; i < gone.memberCount(); i++) {
            final ClientObject member = byId(gone.memberAt(i));
            if (member != null) {
                member.left(id);
            }
        }
        if (gone.externalId() != null) {
            byExternalId.remove(gone.externalId());
        }
    }

    private void joined(final long memberId, final long groupId) {
        final ClientObject member = byId(memberId);
        final ClientObject group = byId(groupId);
        if (member == null || group == null) {
            return;
        }
        member.joined(groupId);
        group.gained(memberId);
    }

    private void left(final long memberId, final long groupId) {
        final ClientObject member = byId(memberId);
        final ClientObject group = byId(groupId);
        if (member != null) {
            member.left(groupId);
        }
        if (group != null) {
            group.lost(memberId);
        }
    }

    private void changed(final ChangeCursor cursor) {
        final ClientObject object = byId(cursor.objectId());
        if (object == null) {
            return;
        }
        final ValueType valueType = cursor.valueType();
        final String key = cursor.propertyKey();
        final String was = "externalId".equals(key) ? object.externalId() : null;
        object.set(key, valueType, numberOf(cursor, valueType), textOf(cursor, valueType));
        if (was != null) {
            byExternalId.remove(was);
        }
        if ("externalId".equals(key) && object.externalId() != null) {
            byExternalId.put(object.externalId(), object);
        }
    }

    private static long numberOf(final ChangeCursor cursor, final ValueType valueType) {
        switch (valueType) {
            case LONG:
            case BOOLEAN:
                return cursor.longValue();
            case DOUBLE:
                return Double.doubleToRawLongBits(cursor.doubleValue());
            default:
                return 0L;
        }
    }

    private static String textOf(final ChangeCursor cursor, final ValueType valueType) {
        return valueType == ValueType.TEXT ? cursor.textValue().toString() : null;
    }
}
