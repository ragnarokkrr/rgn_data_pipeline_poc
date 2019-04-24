package ragna.pipeline.consumer.parsers;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import ragna.pipeline.common.model.Salesman;

import java.math.BigDecimal;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class SalesmanParserTest {

    @ParameterizedTest
    @MethodSource("provideSalesmen")
    public void testParseOK(String input, Salesman expected) {
        SalesmanParser salesmanParser = new SalesmanParser();

        Salesman salesman = salesmanParser.parse(input);

        assertEquals("Salesman", salesman,expected);
    }


    private static Stream<Arguments> provideSalesmen() {
        return Stream.of(
                Arguments.of("001ç1234567891234çPedroç50000",
                        new Salesman("1234567891234", "Pedro", new BigDecimal("50000"))),
                Arguments.of("001ç3245678865434çPauloç40000.99",
                        new Salesman("3245678865434", "Paulo", new BigDecimal("40000.99")))
        );
    }

}
