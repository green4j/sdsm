package io.github.green4j.sdsm;

/**
 * What a port does with the address it declares: offers it, or looks for it.
 */
public enum Role {
    PROVIDE("provide"),
    REQUIRE("require");

    private final String text;

    Role(final String text) {
        this.text = text;
    }

    /**
     * @return how a selector and a change record spell it
     */
    public String text() {
        return text;
    }
}
