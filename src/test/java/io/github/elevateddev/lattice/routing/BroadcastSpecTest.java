package io.github.elevateddev.lattice.routing;

import java.util.function.Function;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BroadcastSpecTest {

    @Test
    void copyWithoutCopierDeclaresReferenceCopyBroadcast() {
        final BroadcastSpec<Payload> spec = BroadcastSpec.copy();

        assertEquals(BroadcastSpec.BroadcastKind.COPY, spec.kind());
        assertNull(spec.copier());
        assertFalse(spec.isolateSlowBranches());
    }

    @Test
    void copyWithCopierExposesBranchCopier() {
        final BroadcastSpec<Payload> spec = BroadcastSpec.copy(Payload::copy);
        final Payload original = new Payload(7);
        final Payload copied = spec.copier().apply(original);

        assertEquals(BroadcastSpec.BroadcastKind.COPY, spec.kind());
        assertEquals(original, copied);
        assertNotSame(original, copied);
        assertFalse(spec.isolateSlowBranches());
    }

    @Test
    void slabHandlesDeclaresHandleBroadcastWithoutPayloadCopier() {
        final BroadcastSpec<Object> spec = BroadcastSpec.slabHandles();

        assertEquals(BroadcastSpec.BroadcastKind.SLAB_HANDLE, spec.kind());
        assertNull(spec.copier());
        assertFalse(spec.isolateSlowBranches());
    }

    @Test
    void branchIsolationReturnsIndependentSpecWithSameBroadcastMode() {
        final Function<Payload, Payload> copier = Payload::copy;
        final BroadcastSpec<Payload> spec = BroadcastSpec.copy(copier);
        final BroadcastSpec<Payload> isolated = spec.withBranchIsolation();

        assertFalse(spec.isolateSlowBranches());
        assertTrue(isolated.isolateSlowBranches());
        assertEquals(spec.kind(), isolated.kind());
        assertSame(copier, isolated.copier());

        final BroadcastSpec<Object> isolatedHandles = BroadcastSpec.slabHandles().withBranchIsolation();
        assertEquals(BroadcastSpec.BroadcastKind.SLAB_HANDLE, isolatedHandles.kind());
        assertNull(isolatedHandles.copier());
        assertTrue(isolatedHandles.isolateSlowBranches());
    }

    @Test
    void copyWithCopierRequiresNonNullCopier() {
        assertThrows(NullPointerException.class, () -> BroadcastSpec.copy(null));
    }

    private record Payload(int value) {

        Payload copy() {
            return new Payload(value);
        }
    }
}
