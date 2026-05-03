package com.lattice.stage;

import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StageLogicTest {

    @Test
    void logicReceivesInputOutputAndContext() throws Exception {
        final RecordingOutput<Integer> output = new RecordingOutput<>();
        final StageContext context = new FixedStageContext("orders", "validate");
        final StageLogic<String, Integer> logic = (input, out, ctx) -> {
            assertSame(output, out);
            assertSame(context, ctx);
            out.push(input.length());
        };

        logic.onMessage("valid", output, context);

        assertEquals(List.of(5), output.items());
    }

    @Test
    void checkedExceptionsRemainPartOfTheCallbackContract() {
        final Exception failure = new Exception("checked");
        final StageLogic<String, String> logic = (input, output, context) -> {
            throw failure;
        };

        final Exception thrown = assertThrows(
            Exception.class,
            () -> logic.onMessage("item", new RecordingOutput<>(), new FixedStageContext("graph", "stage"))
        );
        assertSame(failure, thrown);
    }
}
