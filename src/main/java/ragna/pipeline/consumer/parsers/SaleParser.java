package ragna.pipeline.consumer.parsers;

import org.springframework.stereotype.Component;
import ragna.pipeline.common.exceptions.PipelineException;
import ragna.pipeline.common.model.Sale;
import ragna.pipeline.common.model.SaleItem;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

import static java.util.stream.Collectors.toList;

@Component
public class SaleParser {
    public static final String PREFIX = "003";


    public Sale parse(String input) {
        if (!input.startsWith(PREFIX)) {
            throw new PipelineException("Invalid Sale record id: " + input);
        }

        String[] lines = input.split("ç");

        if (lines.length != 4) {
            throw new PipelineException("Sale Format Exception: " + input);
        }

        String saleItemTrimmed = lines[2]
                .replaceAll("\\[", "")
                .replaceAll("]", "");

        String[] saleItems = saleItemTrimmed.split(",");

        List<SaleItem> itemList = Arrays.stream(saleItems)
                .map(this::parseSaleItem).collect(toList());


        return new Sale(new Long(lines[1])
                , lines[3]
                , itemList);
    }

    private SaleItem parseSaleItem(String s) {
        String[] fields = s.split("-");

        return new SaleItem(new Integer(fields[0])
                , new BigDecimal(fields[1])
                , new BigDecimal(fields[2]));
    }

}
