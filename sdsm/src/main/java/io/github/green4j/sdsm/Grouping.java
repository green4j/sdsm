package io.github.green4j.sdsm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A set cut by a key: the objects a selector holds, a node for every value of the key among
 * them - the links between the same two services carrying the same topic, say - made
 * by the structure when the first member comes and removed when the last one goes. What a
 * group folds of its members is declared once, for every group there is and will be, with
 * {@link Structure#deriveEach(long, int, Fold, Over)}.
 * <p>
 * A key part, like a member's value, is a path: a property of the member, of the object one
 * of its intrinsic ids names - {@code $from.$node}, the node of the port a link runs from - or
 * of its parent on an axis, {@code parent(placement).region}; {@code parent(placement)} alone
 * is the parent's id.
 * A grouping watches the structure, as a view does, and is not part of it; the groups it
 * makes are.
 */
public final class Grouping extends Watcher {

    private final long id;
    private final String name;
    private final String type;
    private final String selectorText;
    private final String[] byText;
    final Selector.Expression selector;
    final MemberPath[] by;
    /** The key each part is said by on a group. */
    final int[] partKeyIds;
    private int reach;
    private boolean[] pathKeys = new boolean[16];

    private final TextObjectMap<Long> groupByKey = new TextObjectMap<>();
    private final LongObjectMap<String> keyOfMember = new LongObjectMap<>();
    private final LongObjectMap<LongSet> membersOfGroup = new LongObjectMap<>();
    private final List<Spec> derivations = new ArrayList<>(2);

    Grouping(final long id,
             final String name,
             final String type,
             final String selectorText,
             final String[] byText,
             final Selector.Expression selector,
             final MemberPath[] by,
             final int[] partKeyIds) {
        this.id = id;
        this.name = name;
        this.type = type;
        this.selectorText = selectorText;
        this.byText = byText.clone();
        this.selector = selector;
        this.by = by;
        this.partKeyIds = partKeyIds;
        this.reach = reachOf(selector);
        for (final MemberPath path : by) {
            reach |= path.reach();
            readsAlong(path);
        }
    }

    public long id() {
        return id;
    }

    public String name() {
        return name;
    }

    /**
     * @return the type of the nodes it makes
     */
    public String type() {
        return type;
    }

    public String selectorText() {
        return selectorText;
    }

    /**
     * @param index below the number of key parts
     * @return the path of a key part as it was given
     */
    public String byAt(final int index) {
        return byText[index];
    }

    @Override
    int reach() {
        return reach;
    }

    /**
     * @param keyId a property key
     * @return whether a group says a part of its key by it
     */
    boolean keyedBy(final int keyId) {
        for (int i = 0; i < partKeyIds.length; i++) {
            if (partKeyIds[i] == keyId) {
                return true;
            }
        }
        return false;
    }

    Long groupOf(final CharSequence key) {
        return groupByKey.get(key);
    }

    String keyOfMember(final long memberId) {
        return keyOfMember.get(memberId);
    }

    LongSet membersOf(final long groupId) {
        return membersOfGroup.get(groupId);
    }

    void addGroup(final String key, final long groupId) {
        groupByKey.put(key, groupId);
        membersOfGroup.put(groupId, new LongSet());
    }

    void removeGroup(final String key, final long groupId) {
        groupByKey.remove(key);
        membersOfGroup.remove(groupId);
    }

    void join(final long memberId, final String key, final long groupId) {
        keyOfMember.put(memberId, key);
        membersOfGroup.get(groupId).add(memberId);
    }

    /**
     * @param memberId a member
     * @return the group it was in, or -1
     */
    long leave(final long memberId) {
        final String key = keyOfMember.remove(memberId);
        if (key == null) {
            return -1L;
        }
        final Long groupId = groupByKey.get(key);
        if (groupId == null) {
            return -1L;
        }
        membersOfGroup.get(groupId).remove(memberId);
        return groupId;
    }

    long[] groupIds() {
        final long[] ids = new long[membersOfGroup.size()];
        for (int i = 0; i < ids.length; i++) {
            ids[i] = membersOfGroup.keyAt(i);
        }
        return ids;
    }

    List<Spec> derivations() {
        return derivations;
    }

    /**
     * @param spec what every group folds
     * @param path the path to the value of each member it folds, or null
     */
    void addDerivation(final Spec spec, final MemberPath path) {
        derivations.add(spec);
        if (path != null) {
            reach |= path.reach();
            readsAlong(path);
        }
    }

    /**
     * @param keyId a property key
     * @return whether a change of that property, on a member or on what a path reaches from it,
     *         can move a member between groups or change what a group folds
     */
    @Override
    boolean reads(final int keyId) {
        return selector.reads(keyId)
                || keyId >= 0 && keyId < pathKeys.length && pathKeys[keyId];
    }

    private void readsAlong(final MemberPath path) {
        if (path.crosses()) {
            markRead(path.hop.keyId);
        }
        markRead(path.keyId);
    }

    private void markRead(final int keyId) {
        if (keyId >= pathKeys.length) {
            pathKeys = Arrays.copyOf(pathKeys, Math.max(keyId + 1, pathKeys.length * 2));
        }
        pathKeys[keyId] = true;
    }

    /**
     * What every group of it folds.
     */
    static final class Spec {
        final int keyId;
        final Fold fold;
        final Over over;

        Spec(final int keyId, final Fold fold, final Over over) {
            this.keyId = keyId;
            this.fold = fold;
            this.over = over;
        }
    }
}
