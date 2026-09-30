package io.github.green4j.sdsm;

/**
 * What a materializer writes through. Every object it names is named by the id the observed
 * world knows it by, so materializing the same observation twice reaches what it made the
 * first time instead of making a second one.
 * <p>
 * The structure is not in reach here: a materializer runs inside a transaction on the
 * structure's own thread, and anything it could do with the structure - start a task, wait on
 * one - would be a deadlock or a second transaction. That is why this is a class the
 * assembly hands out rather than an interface anyone can be given.
 * <p>
 * A property a source says is worth having only at some level is written only while the thing
 * it belongs to is wanted at that level: the write is dropped, not deferred, because a value
 * nobody is paying for is not a value anyone should be shown.
 * <p>
 * Every object reached through this belongs to the area of the observation being materialized
 * and stays alive while the source keeps saying it is there. Removing it is not the
 * materializer's to do, except for {@link #remove(long)}: what an area no longer holds is
 * swept when the source says the area is complete.
 */
public final class Emit {

    private final Structure structure;
    private final LongCounts claims;
    private final int[] supplyThresholds;

    private long[] touched = new long[8];
    private int touchedCount;
    private boolean collecting;

    Emit(final Structure structure, final int[] supplyThresholds) {
        this.structure = structure;
        this.claims = structure.claims();
        this.supplyThresholds = supplyThresholds;
    }

    /**
     * @param objectId the object a value would go on
     * @param keyId    the property
     * @return whether writing it would land, so a materializer can skip working out what
     *         nobody would be shown
     */
    public boolean wanted(final long objectId, final int keyId) {
        return thresholdOf(keyId) <= structure.requiredLevelOrdinal(objectId);
    }

    /**
     * @return the dictionary property names are interned in, so a materializer can hold the
     *         ids of the keys it writes instead of a name per write
     */
    public PropertyKeys keys() {
        return structure.propertyKeys();
    }

    /**
     * @return the dictionary domain names are interned in
     */
    public Domains domains() {
        return structure.domains();
    }

    /**
     * @param externalId the id the node is known by in the world that owns it
     * @param name       name to give it if it is not there yet
     * @param type       type to give it if it is not there yet
     * @return id of the node standing for that thing
     */
    public long node(final CharSequence externalId, final String name, final String type) {
        final StructureObject known = known(externalId, ObjectKind.NODE);
        if (known != null) {
            return touch(known.id());
        }
        return touch(structure.createNode(name, type, externalId).id());
    }

    /**
     * @param nodeId     node the input belongs to
     * @param externalId the id the input is known by
     * @param name       name to give it if it is not there yet
     * @param type       type to give it if it is not there yet
     * @return id of the input
     */
    public long input(final long nodeId, final CharSequence externalId,
                      final String name, final String type) {
        final StructureObject known = known(externalId, ObjectKind.INPUT);
        if (known != null) {
            return touch(known.id());
        }
        return touch(structure.addInput(nodeId, name, type, externalId).id());
    }

    /**
     * @param nodeId     node the output belongs to
     * @param externalId the id the output is known by
     * @param name       name to give it if it is not there yet
     * @param type       type to give it if it is not there yet
     * @return id of the output
     */
    public long output(final long nodeId, final CharSequence externalId,
                       final String name, final String type) {
        final StructureObject known = known(externalId, ObjectKind.OUTPUT);
        if (known != null) {
            return touch(known.id());
        }
        return touch(structure.addOutput(nodeId, name, type, externalId).id());
    }

    /**
     * Puts a node under another on an axis, or leaves it there if it is already there - see
     * {@link Structure#contain(long, long, String)}.
     *
     * @param parentId the node that holds
     * @param childId  the node held
     * @param axis     the axis
     */
    public void contain(final long parentId, final long childId, final String axis) {
        structure.contain(parentId, childId, axis);
    }

    /**
     * Takes a node from under another, if it is there.
     *
     * @param parentId the node that held it
     * @param childId  the node
     */
    public void uncontain(final long parentId, final long childId) {
        structure.uncontain(parentId, childId);
    }

    public void provide(final long portId, final CharSequence address) {
        structure.provide(portId, address);
    }

    public void provide(final long portId, final CharSequence address, final int domainId) {
        structure.provide(portId, address, domainId);
    }

    public void require(final long portId, final CharSequence address) {
        structure.require(portId, address);
    }

    public void require(final long portId, final CharSequence address, final int domainId) {
        structure.require(portId, address, domainId);
    }

    public void clearAddress(final long portId) {
        structure.clearAddress(portId);
    }

    public void setLong(final long objectId, final int keyId, final long value) {
        if (!wanted(objectId, keyId)) {
            return;
        }
        structure.setLong(objectId, keyId, value);
    }

    public void setDouble(final long objectId, final int keyId, final double value) {
        if (!wanted(objectId, keyId)) {
            return;
        }
        structure.setDouble(objectId, keyId, value);
    }

    public void setBoolean(final long objectId, final int keyId, final boolean value) {
        if (!wanted(objectId, keyId)) {
            return;
        }
        structure.setBoolean(objectId, keyId, value);
    }

    public void setText(final long objectId, final int keyId, final String value) {
        if (!wanted(objectId, keyId)) {
            return;
        }
        structure.setText(objectId, keyId, value);
    }

    /**
     * Declares a property the fold of what {@code over} names - see
     * {@link Structure#derive(long, int, Fold, Over)}. Declaring it with every observation
     * costs a look: the same declaration twice is nothing.
     *
     * @param objectId the object
     * @param keyId    what to write on it
     * @param fold     how the summands come to one value
     * @param over     what the summands are
     */
    public void derive(final long objectId, final int keyId, final Fold fold, final Over over) {
        structure.derive(objectId, keyId, fold, over);
    }

    public void removeProperty(final long objectId, final int keyId) {
        structure.removeProperty(objectId, keyId);
    }

    /**
     * Lets go of an object without waiting for its area to be swept - what an observation that
     * has lost a part of itself does to that part. It goes at once if nothing says it is there,
     * when the observation is done if only this one did, and not at all while another does.
     *
     * @param objectId the object to let go of
     */
    public void remove(final long objectId) {
        untouch(objectId);
        if (claims.countOf(objectId) == 0) {
            structure.remove(objectId);
        }
    }

    private int thresholdOf(final int keyId) {
        return keyId < supplyThresholds.length ? supplyThresholds[keyId] : 0;
    }

    private StructureObject known(final CharSequence externalId, final ObjectKind expected) {
        final StructureObject found = structure.findByExternalId(externalId);
        if (found != null && found.kind() != expected) {
            throw new IllegalArgumentException(
                    "External id '" + externalId + "' is already held by "
                            + found.kind() + " " + found.id());
        }
        if (found != null && structure.isGroup(found.id())) {
            throw new IllegalArgumentException(
                    "External id '" + externalId + "' is a group's, not a source's");
        }
        return found;
    }

    private long touch(final long objectId) {
        if (!collecting) {
            return objectId;
        }
        for (int i = 0; i < touchedCount; i++) {
            if (touched[i] == objectId) {
                return objectId;
            }
        }
        if (touchedCount == touched.length) {
            final long[] grown = new long[touchedCount * 2];
            System.arraycopy(touched, 0, grown, 0, touchedCount);
            touched = grown;
        }
        touched[touchedCount++] = objectId;
        return objectId;
    }

    private void untouch(final long objectId) {
        for (int i = 0; i < touchedCount; i++) {
            if (touched[i] == objectId) {
                touched[i] = touched[--touchedCount];
                return;
            }
        }
    }

    void beginObservation() {
        collecting = true;
        touchedCount = 0;
    }

    void endObservation() {
        collecting = false;
    }

    long[] touchedIds() {
        return touched;
    }

    int touchedCount() {
        return touchedCount;
    }
}
