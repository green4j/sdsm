package io.github.green4j.sdsm;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Common base for everything addressable in a Structure.
 * <p>
 * Properties are read and written by key id, typed, without boxing: {@code id}, {@code name},
 * {@code type}, {@code kind} and {@code externalId} are answered from the object's own fields,
 * everything else from its {@link PropertyStore}. The two are one namespace, so a selector and
 * a change record treat an intrinsic key exactly like any other.
 * <p>
 * Only the structure's own thread changes an object. {@link #id()}, {@link #name()},
 * {@link #type()}, {@link #kind()} and {@link #externalId()} never change and can be read
 * anywhere; everything else is read inside {@link Structure#run(Runnable)} or
 * {@link Structure#submit(java.util.function.Supplier)}.
 */
public abstract class StructureObject {
    private final long id;
    private final String name;
    private final String type;
    private final String externalId;

    private final PropertyStore properties = new PropertyStore();

    private byte requiredLevel = (byte) DetailLevel.FINE.ordinal();

    private Derivation[] derivations;

    StructureObject(final long id,
                    final String name,
                    final String type,
                    final String externalId) {
        Objects.requireNonNull(name, "name is required");
        Objects.requireNonNull(type, "type is required");

        this.id = id;
        this.name = name;
        this.type = type;
        this.externalId = externalId;
    }

    public long id() {
        return id;
    }

    public String name() {
        return name;
    }

    public String type() {
        return type;
    }

    public abstract ObjectKind kind();

    /**
     * @return the id the thing this object models carries in the world that owns it, or null
     *         if the object models nothing outside - a parent the structure drew itself, say
     */
    public String externalId() {
        return externalId;
    }

    /**
     * @param keyId property key id
     * @return the source that wrote the value held under this key, or
     *         {@link Structure#NO_SOURCE}
     */
    public int sourceOf(final int keyId) {
        return properties.sourceOf(keyId);
    }

    /**
     * @return how much of this object anyone wants; everything, until a client says otherwise
     */
    public DetailLevel requiredLevel() {
        return DetailLevel.of(requiredLevel);
    }

    int requiredLevelOrdinal() {
        return requiredLevel;
    }

    boolean requireLevel(final int ordinal) {
        if (requiredLevel == ordinal) {
            return false;
        }
        requiredLevel = (byte) ordinal;
        return true;
    }

    PropertyStore properties() {
        return properties;
    }

    /**
     * @return what the structure works out on this object, or null
     */
    Derivation[] derivations() {
        return derivations;
    }

    /**
     * @param keyId a property
     * @return how the structure works it out, or null if it is not derived
     */
    Derivation derivation(final int keyId) {
        for (int i = 0; derivations != null && i < derivations.length; i++) {
            if (derivations[i].keyId == keyId) {
                return derivations[i];
            }
        }
        return null;
    }

    /**
     * @param keyId a property
     * @return whether the structure writes it, rather than a source
     */
    boolean derives(final int keyId) {
        for (int i = 0; derivations != null && i < derivations.length; i++) {
            if (derivations[i].derives(keyId)) {
                return true;
            }
        }
        return false;
    }

    void addDerivation(final Derivation derivation) {
        final int count = derivations == null ? 0 : derivations.length;
        final Derivation[] grown = new Derivation[count + 1];
        if (count > 0) {
            System.arraycopy(derivations, 0, grown, 0, count);
        }
        grown[count] = derivation;
        derivations = grown;
    }

    /**
     * @return how many key ids this object answers from its own fields; they are the first ids
     *         of {@link PropertyKeys}, in order
     */
    int intrinsicKeyCount() {
        return PropertyKeys.AXIS;
    }

    int propertyCount() {
        return intrinsicKeyCount() + properties.size();
    }

    int propertyKeyIdAt(final int index) {
        final int intrinsics = intrinsicKeyCount();
        return index < intrinsics ? index : properties.keyIdAt(index - intrinsics);
    }

    public ValueType valueTypeOf(final int keyId) {
        switch (keyId) {
            case PropertyKeys.ID:
                return ValueType.LONG;
            case PropertyKeys.NAME:
            case PropertyKeys.TYPE:
            case PropertyKeys.KIND:
                return ValueType.TEXT;
            case PropertyKeys.EXTERNAL_ID:
                return externalId == null ? ValueType.ABSENT : ValueType.TEXT;
            default:
                return properties.typeOf(keyId);
        }
    }

    public long longValueOf(final int keyId) {
        if (keyId == PropertyKeys.ID) {
            return id;
        }
        return properties.numberOf(keyId);
    }

    public double doubleValueOf(final int keyId) {
        return Double.longBitsToDouble(properties.numberOf(keyId));
    }

    public boolean booleanValueOf(final int keyId) {
        return properties.numberOf(keyId) != 0L;
    }

    public CharSequence textValueOf(final int keyId) {
        switch (keyId) {
            case PropertyKeys.NAME:
                return name;
            case PropertyKeys.TYPE:
                return type;
            case PropertyKeys.KIND:
                return kind().name();
            case PropertyKeys.EXTERNAL_ID:
                return externalId;
            default:
                return properties.textOf(keyId);
        }
    }

    /**
     * @param keys the naming dictionary of the owning structure
     * @return every property this object holds, intrinsics first, boxed for a caller outside
     *         the change protocol
     */
    Map<String, Object> propertiesSnapshot(final PropertyKeys keys) {
        return snapshotFrom(keys, 0);
    }

    private Map<String, Object> snapshotFrom(final PropertyKeys keys, final int firstIndex) {
        final Map<String, Object> snapshot = new LinkedHashMap<>();
        final int count = propertyCount();
        for (int i = firstIndex; i < count; i++) {
            final int keyId = propertyKeyIdAt(i);
            final ValueType valueType = valueTypeOf(keyId);
            if (valueType == ValueType.ABSENT) {
                continue;
            }
            snapshot.put(keys.nameOf(keyId), boxedValueOf(keyId, valueType));
        }
        return snapshot;
    }

    private Object boxedValueOf(final int keyId, final ValueType valueType) {
        switch (valueType) {
            case LONG:
                return Long.valueOf(longValueOf(keyId));
            case DOUBLE:
                return Double.valueOf(doubleValueOf(keyId));
            case BOOLEAN:
                return Boolean.valueOf(booleanValueOf(keyId));
            case TEXT:
                return textValueOf(keyId).toString();
            default:
                return null;
        }
    }

    @Override
    public String toString() {
        return kind().name() + "(" + id + ", " + name + ")";
    }
}
