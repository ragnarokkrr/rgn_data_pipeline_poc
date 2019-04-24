package ragna.pipeline.consumer.parsers;

import org.springframework.stereotype.Component;
import ragna.pipeline.common.exceptions.PipelineException;
import ragna.pipeline.common.model.Salesman;

import java.math.BigDecimal;

@Component
public class SalesmanParser {

    public static final String PREFIX = "001";

    public Salesman parse(String input) {

        if (!input.startsWith(PREFIX)) {
            throw new PipelineException("Invalid Salesman record id: " + input);
        }

        String[] lines = input.split("ç");

        if (lines.length != 4) {
            throw new PipelineException("Salesman Format Exception: " + input);
        }


        Salesman salesman = new Salesman(lines[1], lines[2], new BigDecimal(lines[3]));
        return salesman;
    }
}
