package io.github.elevateddev.lattice.stage;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StageHandleTest {

    @Test
    void handleStoresTrimmedNonBlankStageName() {
        final StageHandle handle = new StageHandle(" validate ");

        assertEquals("validate", handle.name());
    }

    @Test
    void handleRejectsMissingOrBlankNames() {
        assertThrows(NullPointerException.class, () -> new StageHandle(null));
        assertThrows(IllegalArgumentException.class, () -> new StageHandle(""));
        assertThrows(IllegalArgumentException.class, () -> new StageHandle("   "));
    }
}
