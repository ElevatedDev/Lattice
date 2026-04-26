package com.lattice.stage;

import java.util.Objects;

/**
 * Lightweight public handle for a named stage.
 */
public final class StageHandle {

    private final String name;

    /**
     * Creates a stage handle.
     */
    public StageHandle(final String name) {
        final String trimmed = Objects.requireNonNull(name, "name").trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException("stage name must not be blank");
        }
        this.name = trimmed;
    }

    /**
     * Returns the stage name.
     */
    public String name() {
        return name;
    }
}
