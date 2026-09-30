package io.github.green4j.sdsm;

/**
 * A link endpoint on a node, answering which node that is. A port may also declare an
 * address: providing it says the thing the port stands for is reachable there, requiring it
 * says the port is looking for it. The structure links a provider to a requirer of the
 * opposite kind at the same address, so neither side has to know the other exists, or to
 * come first.
 * <p>
 * The address is one opaque text. Matching is equality, so the structure never needs its
 * parts, and a caller that wants scheme, host and path spells them into it.
 */
public abstract class Port extends StructureObject {

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
     * @return the address this port provides or requires, or null if it declares none
     */
    public String address() {
        return declaration != null ? declaration.address : null;
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
     * An address a port declares, what it does with it, and in which copy of the world.
     */
    static final class Declaration {
        final String address;
        final Role role;
        final int domainId;
        /** The domain's name, or null for {@link Domains#NO_DOMAIN}. */
        final String domainName;

        Declaration(final String address, final Role role, final int domainId, final String domainName) {
            this.address = address;
            this.role = role;
            this.domainId = domainId;
            this.domainName = domainId == Domains.NO_DOMAIN ? null : domainName;
        }

        boolean sameAs(final CharSequence otherAddress, final Role otherRole, final int otherDomainId) {
            return role == otherRole && domainId == otherDomainId
                    && TextObjectMap.sameText(address, otherAddress);
        }
    }
}
