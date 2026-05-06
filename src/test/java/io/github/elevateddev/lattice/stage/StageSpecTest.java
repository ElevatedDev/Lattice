package io.github.elevateddev.lattice.stage;

import io.github.elevateddev.lattice.placement.PinPolicy;
import io.github.elevateddev.lattice.wait.WaitSpec;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StageSpecTest {

    @Test
    void singleThreadedSpecUsesDocumentedDefaults() {
        final StageSpec spec = StageSpec.singleThreaded();

        assertEquals(StageSpec.StageExecution.SINGLE_THREADED, spec.execution());
        assertEquals(BatchPolicy.BatchKind.DISABLED, spec.batchPolicy().kind());
        assertEquals(PinPolicy.PinKind.NONE, spec.pinPolicy().kind());
        assertEquals(WaitSpec.WaitKind.PHASED, spec.waitSpec().kind());
    }

    @Test
    void builderMethodsReturnCopiesAndPreserveUnchangedOptions() {
        final StageSpec base = StageSpec.singleThreaded();
        final BatchPolicy batch = BatchPolicy.maxItems(8);
        final PinPolicy pin = PinPolicy.cpu(1);
        final WaitSpec wait = WaitSpec.busySpin();

        final StageSpec batched = base.batch(batch);
        final StageSpec pinned = batched.pin(pin);
        final StageSpec waited = pinned.wait(wait);

        assertNotSame(base, batched);
        assertNotSame(batched, pinned);
        assertNotSame(pinned, waited);
        assertEquals(BatchPolicy.BatchKind.DISABLED, base.batchPolicy().kind());
        assertSame(batch, batched.batchPolicy());
        assertSame(batch, pinned.batchPolicy());
        assertEquals(PinPolicy.PinKind.NONE, batched.pinPolicy().kind());
        assertSame(pin, pinned.pinPolicy());
        assertSame(pin, waited.pinPolicy());
        assertEquals(WaitSpec.WaitKind.PHASED, pinned.waitSpec().kind());
        assertSame(wait, waited.waitSpec());
    }

    @Test
    void builderMethodsRejectNullOptions() {
        final StageSpec spec = StageSpec.singleThreaded();

        assertThrows(NullPointerException.class, () -> spec.batch(null));
        assertThrows(NullPointerException.class, () -> spec.pin(null));
        assertThrows(NullPointerException.class, () -> spec.wait(null));
    }
}
