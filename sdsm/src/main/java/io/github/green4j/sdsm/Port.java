package io.github.green4j.sdsm;

import java.util.Collection;

/**
 * A link endpoint on a node, answering which node that is. A port may also declare an
 * address: providing it says the thing the port stands for is reachable there, requiring it
 * says the port is looking for it. The structure links a provider to a requirer of the
 * opposite kind at the same address, so neither side has to know the other exists, or to
 * come first.
 * <p>
 * The address is one opaque text. Matching is equality, so the structure never needs its
 * parts, and a caller that wants scheme, host and path spells them into it. A port may declare
 * several, each matched on its own: one input that reads every stream of a store meets the
 * output of each.
 */
public abstract class Port extends StructureObject {

    /**
     * Between a node's external id and the name of one of its inputs.
     */
    public static final char INPUT_SEPARATOR = '<';
    /**
     * Between a node's external id and the name of one of its outputs.
     */
    public static final char OUTPUT_SEPARATOR = '>';

    private final Node owningNode;

    private Declaration declaration;
    private int links;

    Port(final long id, final String name, final String type, final Node owningNode, final String externalId) {
        super(id, name, type, externalId);
        this.owningNode = owningNode;
    }

    Node owningNode() {
        return owningNode;
    }

    /**
     * @return the address this port provides or requires, its addresses separated by a space if
     *         it declares several, or null if it declares none
     */
    public String address() {
        return declaration != null ? declaration.shown : null;
    }

    /**
     * @return how many addresses it declares
     */
    public int addressCount() {
        return declaration != null ? declaration.addresses.length : 0;
    }

    /**
     * @param index below {@link #addressCount()}; the addresses are in their natural order
     * @return one of its addresses
     */
    public String addressAt(final int index) {
        return declaration.addresses[index];
    }

    /**
     * @return the copy of the world the address is declared in, {@link Domains#NO_DOMAIN}
     *         unless the caller named one
     */
    public int domain() {
        return declaration != null ? declaration.domainId : Domains.NO_DOMAIN;
    }

    /**
     * @return what it does with the address, or null if it declares none
     */
    public Role role() {
        return declaration != null ? declaration.role : null;
    }

    Declaration declaration() {
        return declaration;
    }

    /**
     * @return how many links end here, so a selector finds a requirer nobody answers:
     *         {@code input[role=require] & *[links=0]}
     */
    public int links() {
        return links;
    }

    void declare(final Declaration made) {
        this.declaration = made;
    }

    void undeclare() {
        this.declaration = null;
    }

    void addLinks(final int delta) {
        links += delta;
    }

    @Override
    int intrinsicKeyCount() {
        return PropertyKeys.LINKS + 1;
    }

    @Override
    public ValueType valueTypeOf(final int keyId) {
        switch (keyId) {
            case PropertyKeys.ADDRESS:
            case PropertyKeys.ROLE:
                return declaration == null ? ValueType.ABSENT : ValueType.TEXT;
            case PropertyKeys.DOMAIN:
                return declaration == null || declaration.domainName == null
                        ? ValueType.ABSENT : ValueType.TEXT;
            case PropertyKeys.NODE:
            case PropertyKeys.LINKS:
                return ValueType.LONG;
            default:
                return super.valueTypeOf(keyId);
        }
    }

    @Override
    public long longValueOf(final int keyId) {
        switch (keyId) {
            case PropertyKeys.NODE:
                return owningNode.id();
            case PropertyKeys.LINKS:
                return links;
            default:
                return super.longValueOf(keyId);
        }
    }

    @Override
    public CharSequence textValueOf(final int keyId) {
        switch (keyId) {
            case PropertyKeys.ADDRESS:
                return address();
            case PropertyKeys.ROLE:
                return declaration != null ? declaration.role.text() : null;
            case PropertyKeys.DOMAIN:
                return declaration != null ? declaration.domainName : null;
            default:
                return super.textValueOf(keyId);
        }
    }

    /**
     * The addresses a port declares, what it does with them, and in which copy of the world.
     */
    static final class Declaration {
        /** Sorted, each once. */
        final String[] addresses;
        final String shown;
        final Role role;
        final int domainId;
        /** The domain's name, or null for {@link Domains#NO_DOMAIN}. */
        final String domainName;

        Declaration(final String[] addresses, final Role role, final int domainId, final String domainName) {
            this.addresses = addresses;
            this.shown = addresses.length == 1 ? addresses[0] : String.join(" ", addresses);
            this.role = role;
            this.domainId = domainId;
            this.domainName = domainId == Domains.NO_DOMAIN ? null : domainName;
        }

        boolean sameAs(final CharSequence otherAddress, final Role otherRole, final int otherDomainId) {
            return role == otherRole && domainId == otherDomainId
                    && addresses.length == 1 && TextObjectMap.sameText(addresses[0], otherAddress);
        }

        /**
         * Without allocating, so an observation repeating itself costs nothing; one that names an
         * address twice is not taken for the same here.
         *
         * @param otherAddresses the addresses, in any order
         * @param otherRole      what is done with them
         * @param otherDomainId  in which copy of the world
         * @return whether this declares them, each once, alike
         */
        boolean sameAs(final Collection<? extends CharSequence> otherAddresses,
                       final Role otherRole,
                       final int otherDomainId) {
            if (role != otherRole || domainId != otherDomainId || addresses.length != otherAddresses.size()) {
                return false;
            }
            for (final CharSequence address : otherAddresses) {
                if (!declares(address)) {
                    return false;
                }
            }
            return true;
        }

        boolean declares(final CharSequence address) {
            int low = 0;
            int high = addresses.length - 1;
            while (low <= high) {
                final int middle = (low + high) >>> 1;
                final int order = CharSequence.compare(addresses[middle], address);
                if (order < 0) {
                    low = middle + 1;
                } else if (order > 0) {
                    high = middle - 1;
                } else {
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * A port of a node with an external id is known by it, the side and its own name:
     * {@code node<name} for an input, {@code node>name} for an output. The structure keeps it;
     * a source that measures the port spells the same one.
     *
     * @param out            where to append it
     * @param nodeExternalId the node's
     * @param side           {@link ObjectKind#INPUT} or {@link ObjectKind#OUTPUT}
     * @param name           the port's name, one to a side of the node
     * @return {@code out}
     */
    public static StringBuilder appendExternalId(final StringBuilder out,
                                                 final CharSequence nodeExternalId,
                                                 final ObjectKind side,
                                                 final CharSequence name) {
        if (side != ObjectKind.INPUT && side != ObjectKind.OUTPUT) {
            throw new IllegalArgumentException("A port is an input or an output: " + side);
        }
        return out.append(nodeExternalId)
                .append(side == ObjectKind.INPUT ? INPUT_SEPARATOR : OUTPUT_SEPARATOR)
                .append(name);
    }
}
