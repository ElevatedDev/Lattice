package com.lattice.internal.graph;

import com.lattice.graph.GraphPlan;
import com.lattice.graph.SourceMode;
import com.lattice.stage.StageSpec;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class NodeDefinitionTest {

    @Test
    void recordCapturesDeclaredGraphNodeMetadata() {
        final StageSpec spec = StageSpec.singleThreaded();
        final NodeDefinition node = new NodeDefinition(
            "stage",
            GraphPlan.NodeKind.STAGE,
            Integer.class,
            String.class,
            (input, output, context) -> output.push(input.toString()),
            null,
            null,
            spec,
            3,
            SourceMode.SINGLE_PRODUCER,
            false,
            null,
            null,
            null,
            null,
            null,
            null
        );

        assertEquals("stage", node.name());
        assertEquals(GraphPlan.NodeKind.STAGE, node.kind());
        assertEquals(Integer.class, node.inputType());
        assertEquals(String.class, node.outputType());
        assertSame(spec, node.spec());
        assertEquals(3, node.declarationOrder());
        assertEquals(SourceMode.SINGLE_PRODUCER, node.sourceMode());
    }
}
