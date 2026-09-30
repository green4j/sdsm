package io.github.green4j.sdsm;

/**
 * A directed edge from an output to an input, answering which ports those are.
 */
public final class Link extends StructureObject {

    private Output fromOutput;
    private Input toInput;
    private final boolean derived;

    Link(final long id,
                final String name,
                final String type,
                final Output fromOutput,
                final Input toInput) {
        this(id, name, type, fromOutput, toInput, false, null);
    }

    Link(final long id,
            final String name,
            final String type,
            final Output fromOutput,
            final Input toInput,
            final boolean derived,
            final String externalId) {
        super(id, name, type, externalId);
        this.fromOutput = fromOutput;
        this.toInput = toInput;
        this.derived = derived;
    }

    Output fromOutput() {
        return fromOutput;
    }

    Input toInput() {
        return toInput;
    }

    /**
     * @return whether the structure drew this link from an address two ports declared, in
     *         which case it lives as long as both declarations do and cannot be removed
     *         on its own
     */
    public boolean isDerived() {
        return derived;
    }

    void retargetFrom(final Output newFromOutput) {
        this.fromOutput = newFromOutput;
    }

    void retargetTo(final Input newToInput) {
        this.toInput = newToInput;
    }

    @Override
    public ObjectKind kind() {
        return ObjectKind.LINK;
    }

    @Override
    int intrinsicKeyCount() {
        return PropertyKeys.TO + 1;
    }

    @Override
    public ValueType valueTypeOf(final int keyId) {
        switch (keyId) {
            case PropertyKeys.FROM:
                return fromOutput == null ? ValueType.ABSENT : ValueType.LONG;
            case PropertyKeys.TO:
                return toInput == null ? ValueType.ABSENT : ValueType.LONG;
            default:
                return super.valueTypeOf(keyId);
        }
    }

    @Override
    public long longValueOf(final int keyId) {
        switch (keyId) {
            case PropertyKeys.FROM:
                return fromOutput.id();
            case PropertyKeys.TO:
                return toInput.id();
            default:
                return super.longValueOf(keyId);
        }
    }
}
