package com.lattice.stage;

import java.util.Objects;

public final class StageHandle {

    private final String name;

    public StageHandle(final String name) {
        final String trimmed = Objects.requireNonNull(name, "name").trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException("stage name must not be blank");
        }
        this.name = trimmed;
    }

    public String name() {
        return name;
    }
}
