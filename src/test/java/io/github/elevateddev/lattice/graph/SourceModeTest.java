package io.github.elevateddev.lattice.graph;

import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SourceModeTest {

    @Test
    void exposesMultiProducerDefaultAndSingleProducerContractModes() {
        assertEquals(List.of(SourceMode.MULTI_PRODUCER, SourceMode.SINGLE_PRODUCER), List.of(SourceMode.values()));
        assertEquals(SourceMode.MULTI_PRODUCER, SourceMode.valueOf("MULTI_PRODUCER"));
        assertEquals(SourceMode.SINGLE_PRODUCER, SourceMode.valueOf("SINGLE_PRODUCER"));
    }
}
