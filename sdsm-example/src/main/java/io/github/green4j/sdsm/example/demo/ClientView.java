package io.github.green4j.sdsm.example.demo;

import io.github.green4j.sdsm.BatchSubscriber;
import io.github.green4j.sdsm.ChangeCursor;
import io.github.green4j.sdsm.ObjectKind;
import io.github.green4j.sdsm.StructureBatch;

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

    @Override
    public synchronized void onBatch(final StructureBatch batch) {
        if (batch.isInitialSnapshot()) {
            byId.clear();
            byExternalId.clear();
        }
        final ChangeCursor cursor = batch.cursor();
        while (cursor.next()) {
            switch (cursor.changeKind()) {
                case ADDED:
                    byId.putIfAbsent(cursor.objectId(), new ClientObject(cursor.objectId(), cursor.objectKind()));
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
    }

    @Override
    public void onError(final Throwable reason) {
        reason.printStackTrace(System.err);
    }

    public synchronized ClientObject byId(final long id) {
        return byId.get(id);
    }

    public synchronized ClientObject byExternalId(final String externalId) {
        return byExternalId.get(externalId);
    }

    /**
     * @param type what to look for
     * @return the objects of that type, in no particular order
     */
    public synchronized List<ClientObject> ofType(final String type) {
        final List<ClientObject> found = new ArrayList<>();
        for (final ClientObject object : byId.values()) {
            if (type.equals(object.type())) {
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
            if (axis.equals(object.axis()) && object.groups().isEmpty()) {
                roots.add(object);
            }
        }
        return roots;
    }

    private void removed(final long id) {
        final ClientObject gone = byId.remove(id);
        if (gone == null) {
            return;
        }
        for (final long group : gone.groups()) {
            final ClientObject held = byId.get(group);
            if (held != null) {
                held.lost(id);
            }
        }
        for (final long member : gone.members()) {
            final ClientObject held = byId.get(member);
            if (held != null) {
                held.left(id);
            }
        }
        if (gone.externalId() != null) {
            byExternalId.remove(gone.externalId());
        }
    }

    private void joined(final long memberId, final long groupId) {
        final ClientObject member = byId.get(memberId);
        final ClientObject group = byId.get(groupId);
        if (member != null && group != null) {
            member.joined(groupId);
            group.gained(memberId);
        }
    }

    private void left(final long memberId, final long groupId) {
        final ClientObject member = byId.get(memberId);
        final ClientObject group = byId.get(groupId);
        if (member != null) {
            member.left(groupId);
        }
        if (group != null) {
            group.lost(memberId);
        }
    }

    // $externalId comes once, when the object does, and never changes
    private void changed(final ChangeCursor cursor) {
        final ClientObject object = byId.get(cursor.objectId());
        if (object == null) {
            return;
        }
        object.set(cursor.propertyKey(), valueOf(cursor));
        if ("$externalId".equals(cursor.propertyKey()) && object.externalId() != null) {
            byExternalId.put(object.externalId(), object);
        }
    }

    private static Object valueOf(final ChangeCursor cursor) {
        switch (cursor.valueType()) {
            case LONG:
                return cursor.longValue();
            case DOUBLE:
                return cursor.doubleValue();
            case BOOLEAN:
                return cursor.booleanValue();
            case TEXT:
                return cursor.textValue().toString();
            default:
                return null;
        }
    }
}
