package ragna.pipeline.consumer.parsers;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import ragna.pipeline.common.model.Sale;
import ragna.pipeline.common.model.SaleItem;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class SaleParserTest {
    @ParameterizedTest
    @MethodSource("provideSales")
    public void validLine_ShouldBeParsed(String input, Sale expected) {
        SaleParser saleParser = new SaleParser();

        Sale sale = saleParser.parse(input);

        assertEquals("Sale", sale, expected);
    }


    private static Stream<Arguments> provideSales() {
        return Stream.of(
            Arguments.of("003ç10ç[1-10-100,2-30-2.50,3-40-3.10]çPedro",
                  new Sale(10L, "Pedro"
                          , Arrays.asList(
                          new SaleItem(1, new BigDecimal(10)
                                  , new BigDecimal(100)),
                          new SaleItem(2, new BigDecimal(30)
                                  , new BigDecimal("2.50")),
                          new SaleItem(3, new BigDecimal(40)
                                  , new BigDecimal("3.10"))
                  ))),
            Arguments.of("003ç08ç[1-34-10,2-33-1.50,3-40-0.10]çPaulo",
                new Sale(8L, "Paulo"
                    , Arrays.asList(
                        new SaleItem(1, new BigDecimal("34")
                        , new BigDecimal("10")),
                        new SaleItem(2, new BigDecimal("33")
                            , new BigDecimal("1.50")),
                        new SaleItem(3, new BigDecimal("40")
                            , new BigDecimal("0.10"))
                )))
        );
    }
}
