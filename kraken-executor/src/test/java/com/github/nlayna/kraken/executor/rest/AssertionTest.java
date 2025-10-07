package com.github.nlayna.kraken.executor.rest;

import com.github.nlayna.kraken.executor.utils.Assertion;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
class AssertionTest {

    @Test
    void evaluate() {
        var ass = new Assertion("$.statusCode=='0'");
        var json = """
                { "statusCode" : "0"}
                """;
        assertTrue(ass.evaluate(json));

    }

}