package io.github.green4j.sdsm;

public final class Input extends Port {

    Input(final long id,
                 final String name,
                 final String type,
                 final Node owningNode,
                 final String externalId) {
        super(id, name, type, owningNode, externalId);
    }

    @Override
    public ObjectKind kind() {
        return ObjectKind.INPUT;
    }
}
