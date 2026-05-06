package io.github.elevateddev.lattice.graph;

/**
 * Graph-level fusion controls.
 * <p>
 * These options allow optimizations when the planner can prove they preserve
 * graph semantics. They do not force unsafe source inlining or placement
 * violations.
 */
public final class FusionSpec {

    private static final FusionSpec DEFAULTS = new FusionSpec(true, false, false, false);
    private static final FusionSpec DISABLED = DEFAULTS.enabled(false);

    private final boolean enabled;
    private final boolean inlineSources;
    private final boolean elideInlineSourcePhysicalPath;
    private final boolean validateTypes;

    private FusionSpec(
        final boolean enabled,
        final boolean inlineSources,
        final boolean elideInlineSourcePhysicalPath,
        final boolean validateTypes
    ) {
        this.enabled = enabled;
        this.inlineSources = inlineSources;
        this.elideInlineSourcePhysicalPath = elideInlineSourcePhysicalPath;
        this.validateTypes = validateTypes;
    }

    /**
     * Enables normal downstream fusion and keeps source-inline execution off.
     */
    public static FusionSpec defaults() {
        return DEFAULTS;
    }

    /**
     * Disables all runtime fusion and uses the physical graph shape.
     */
    public static FusionSpec disabled() {
        return DISABLED;
    }

    /**
     * Returns a copy that allows or disables graph fusion.
     */
    public FusionSpec enabled(final boolean enabled) {
        return new FusionSpec(enabled, inlineSources, elideInlineSourcePhysicalPath, validateTypes);
    }

    /**
     * Returns a copy that allows source-inline execution when planner safety
     * checks pass.
     */
    public FusionSpec inlineSources(final boolean inlineSources) {
        return new FusionSpec(enabled, inlineSources, elideInlineSourcePhysicalPath, validateTypes);
    }

    /**
     * Returns a copy that allows physical source edge/worker elision for
     * eligible source-inline chains.
     */
    public FusionSpec elideInlineSourcePhysicalPath(final boolean elideInlineSourcePhysicalPath) {
        return new FusionSpec(enabled, inlineSources, elideInlineSourcePhysicalPath, validateTypes);
    }

    /**
     * Returns a copy that enables runtime type assertions on fused direct-call
     * paths.
     */
    public FusionSpec validateTypes(final boolean validateTypes) {
        return new FusionSpec(enabled, inlineSources, elideInlineSourcePhysicalPath, validateTypes);
    }

    /**
     * Returns whether graph fusion is enabled.
     */
    public boolean enabled() {
        return enabled;
    }

    /**
     * Returns whether source-inline execution is allowed.
     */
    public boolean inlineSources() {
        return inlineSources;
    }

    /**
     * Returns whether eligible source-inline physical paths may be elided.
     */
    public boolean elideInlineSourcePhysicalPath() {
        return elideInlineSourcePhysicalPath;
    }

    /**
     * Returns whether fused direct-call type assertions are enabled.
     */
    public boolean validateTypes() {
        return validateTypes;
    }
}
