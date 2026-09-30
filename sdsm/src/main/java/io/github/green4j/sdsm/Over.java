package io.github.green4j.sdsm;

import java.util.Arrays;
import java.util.Objects;

/**
 * What a derived property is folded over: properties of the object itself - what several
 * sources each say about one thing - a property of its children, or both, as the worst of what
 * is said about a component and of what it runs on; or, on a node a {@link Grouping} made, a
 * property of its members.
 */
public final class Over {

    private static final int[] NONE = new int[0];

    final int[] ownKeyIds;
    final int childKeyId;
    final String childType;
    final String memberPath;

    private Over(final int[] ownKeyIds, final int childKeyId, final String childType, final String memberPath) {
        this.ownKeyIds = ownKeyIds;
        this.childKeyId = childKeyId;
        this.childType = childType;
        this.memberPath = memberPath;
    }

    /**
     * @param keyIds properties of the object itself
     * @return a fold over them
     */
    public static Over keys(final int... keyIds) {
        if (keyIds == null || keyIds.length == 0) {
            throw new IllegalArgumentException("keyIds are required");
        }
        for (final int keyId : keyIds) {
            requireKey(keyId);
        }
        return new Over(keyIds.clone(), -1, null, null);
    }

    /**
     * @param keyId what to read off the children
     * @param type  which children are summands, or null for every child
     * @return a fold over the children
     */
    public static Over children(final int keyId, final String type) {
        return new Over(NONE, requireKey(keyId), type, null);
    }

    /**
     * @param path what to read off each member of a group: its own property, {@code rate}, or
     *             its end's, {@code from.rate} - the port a link runs from
     * @return a fold over the members of the group a {@link Grouping} made
     */
    public static Over members(final String path) {
        if (path == null || path.isBlank()) {
            throw new IllegalArgumentException("path is required");
        }
        return new Over(NONE, -1, null, path);
    }

    /**
     * @param keyId what to read off the children
     * @param type  which children are summands, or null for every child
     * @return a fold over these properties and the children too
     */
    public Over andChildren(final int keyId, final String type) {
        return new Over(ownKeyIds, requireKey(keyId), type, null);
    }

    boolean overChildren() {
        return childKeyId >= 0;
    }

    boolean overMembers() {
        return memberPath != null;
    }

    /**
     * @return whether it counts its summands: over children or members
     */
    boolean counts() {
        return overChildren() || overMembers();
    }

    boolean readsOwn(final int keyId) {
        for (int i = 0; i < ownKeyIds.length; i++) {
            if (ownKeyIds[i] == keyId) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof Over)) {
            return false;
        }
        final Over over = (Over) other;
        return childKeyId == over.childKeyId && Objects.equals(childType, over.childType)
                && Objects.equals(memberPath, over.memberPath) && Arrays.equals(ownKeyIds, over.ownKeyIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(childKeyId, childType, memberPath, Arrays.hashCode(ownKeyIds));
    }

    /**
     * An intrinsic property changes with the shape of the graph rather than by a write, so no
     * fold could follow it.
     *
     * @param keyId what a fold would read
     * @return the key
     */
    private static int requireKey(final int keyId) {
        if (keyId < 0) {
            throw new IllegalArgumentException("Not a key: " + keyId);
        }
        if (PropertyKeys.isReserved(keyId)) {
            throw new IllegalArgumentException("An intrinsic property is not folded: " + keyId);
        }
        return keyId;
    }
}
