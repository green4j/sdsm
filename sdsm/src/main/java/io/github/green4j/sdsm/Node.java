package io.github.green4j.sdsm;

import java.util.List;

/**
 * A primary object: it owns ports, and may hold nodes on one axis - a region its silos, a silo
 * its components, a component what it runs on - folding them into properties of its own.
 */
public final class Node extends StructureObject {

    private final LongObjectMap<Input> inputsById = new LongObjectMap<>();
    private final LongObjectMap<Output> outputsById = new LongObjectMap<>();
    private final LongObjectMap<Node> children = new LongObjectMap<>();
    private String axis;

    Node(final long id, final String name, final String type, final String externalId) {
        super(id, name, type, externalId);
    }

    @Override
    public ObjectKind kind() {
        return ObjectKind.NODE;
    }

    /**
     * @return the axis it holds children on, or null if it has held none
     */
    public String axis() {
        return axis;
    }

    /**
     * @param holding an axis
     * @return whether it can hold on it: it holds on it already, or on none
     */
    boolean canHoldOn(final String holding) {
        return axis == null || axis.equals(holding);
    }

    /**
     * @param holding the axis of what it is about to hold, one it {@link #canHoldOn(String)}
     */
    void holdOn(final String holding) {
        axis = holding;
    }

    @Override
    int intrinsicKeyCount() {
        return PropertyKeys.AXIS + 1;
    }

    @Override
    public ValueType valueTypeOf(final int keyId) {
        if (keyId == PropertyKeys.AXIS) {
            return axis != null ? ValueType.TEXT : ValueType.ABSENT;
        }
        return super.valueTypeOf(keyId);
    }

    @Override
    public CharSequence textValueOf(final int keyId) {
        if (keyId == PropertyKeys.AXIS) {
            return axis;
        }
        return super.textValueOf(keyId);
    }

    Input input(final long inputId) {
        return inputsById.get(inputId);
    }

    Output output(final long outputId) {
        return outputsById.get(outputId);
    }

    List<Input> inputs() {
        return inputsById.values();
    }

    List<Output> outputs() {
        return outputsById.values();
    }

    LongObjectMap<Input> inputsMap() {
        return inputsById;
    }

    LongObjectMap<Output> outputsMap() {
        return outputsById;
    }

    LongObjectMap<Node> childrenMap() {
        return children;
    }

    /**
     * @param childId a child to let go
     * @return whether it was a child
     */
    boolean removeChild(final long childId) {
        return children.remove(childId) != null;
    }

    long[] childIds() {
        final long[] ids = new long[children.size()];
        for (int i = 0; i < ids.length; i++) {
            ids[i] = children.keyAt(i);
        }
        return ids;
    }
}
