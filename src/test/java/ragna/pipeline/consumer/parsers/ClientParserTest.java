package ragna.pipeline.consumer.parsers;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import ragna.pipeline.common.model.Client;

import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class ClientParserTest {

    @ParameterizedTest
    @MethodSource("provideClients")
    public void validLine_ShouldBeParsed(String input, Client expected) {
        ClientParser clientParser = new ClientParser();

        Client client = clientParser.parse(input);

        assertEquals("Client", client, expected);
    }


    private static Stream<Arguments> provideClients() {
        return Stream.of(
                Arguments.of("002ç2345675434544345çJose da SilvaçRural",
                        new Client("2345675434544345", "Jose da Silva", "Rural")),
                Arguments.of("002ç2345675433444345çEduardo PereiraçRural",
                        new Client("2345675433444345", "Eduardo Pereira", "Rural"))
                );
    }
}
