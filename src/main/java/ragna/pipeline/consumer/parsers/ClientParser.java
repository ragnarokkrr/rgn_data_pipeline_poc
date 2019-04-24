package ragna.pipeline.consumer.parsers;

import org.springframework.stereotype.Component;
import ragna.pipeline.common.exceptions.PipelineException;
import ragna.pipeline.common.model.Client;

@Component
public class ClientParser {

    public static final String PREFIX = "002";

    public Client parse (String input) {
        if (!input.startsWith(PREFIX)) {
            throw new PipelineException("Invalid Client record id: " + input);
        }

        String[] lines = input.split("ç");

        if (lines.length != 4) {
            throw new PipelineException("Client Format Exception: " + input);
        }


        Client client = new Client(lines[1], lines[2], lines[3]);
        return client;
    }
}
