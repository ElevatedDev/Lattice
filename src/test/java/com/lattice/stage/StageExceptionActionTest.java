package com.lattice.stage;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class StageExceptionActionTest {

    @Test
    void exposesDocumentedRuntimeFailureResponses() {
        assertArrayEquals(
            new StageExceptionAction[] {
                StageExceptionAction.FAIL_GRAPH,
                StageExceptionAction.POISON_STAGE
            },
            StageExceptionAction.values()
        );
        assertEquals(StageExceptionAction.FAIL_GRAPH, StageExceptionAction.valueOf("FAIL_GRAPH"));
        assertEquals(StageExceptionAction.POISON_STAGE, StageExceptionAction.valueOf("POISON_STAGE"));
    }
}
