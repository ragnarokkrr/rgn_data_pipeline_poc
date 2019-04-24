package ragna.pipeline.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ragna.pipeline.common.model.Client;
import ragna.pipeline.common.model.RawRecord;
import ragna.pipeline.common.model.Sale;
import ragna.pipeline.common.model.Salesman;
import ragna.pipeline.consumer.parsers.ClientParser;
import ragna.pipeline.consumer.parsers.SaleParser;
import ragna.pipeline.consumer.parsers.SalesmanParser;
import ragna.pipeline.consumer.stats.FileStats;
import ragna.pipeline.director.FilePipelineDirector;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class ConsumeFileContents extends RouteBuilder {

    public static final int DELAY_MIN = 1;
    @Autowired
    private FileStats fileStats;

    @Autowired
    private SaleParser saleParser;

    @Autowired
    private SalesmanParser salesmanParser;

    @Autowired
    private ClientParser clientParser;

    @Autowired
    private FilePipelineDirector filePipelineDirector;

    @Override
    public void configure() throws Exception {
        from("kafka:filetopic?brokers=localhost:9092&groupId=Consumer")
                .routeId("FileConsumer route")
                .process(ConsumeFileContents::unmarshall)
                .errorHandler(deadLetterChannel("kafka:consumererrortopic?brokers=localhost:9092"))
                .choice()
                    .when(header("recordPrefix").isEqualTo("001"))
                        .process(this::processSalesman)
                    .when(header("recordPrefix").isEqualTo("002"))
                        .process(this::processClient)
                    .when(header("recordPrefix").isEqualTo("003"))
                        .process(this::processSales)
                    .when(header("recordPrefix").isEqualTo("004"))
                        .process(this::delayedFinish)
                        .to("kafka:reportertopic?brokers=localhost:9092")
                    .otherwise()
                        .to("kafka:consumererrortopic?brokers=localhost:9092")
                .end()
                ;
    }

    /**
     * Some delay to expect file processing conclusion.
     * @param exchange
     */
    private void delayedFinish(Exchange exchange) {
        delayMinutes(DELAY_MIN);

        RawRecord rawRecord = exchange.getIn().getBody(RawRecord.class);
        log.info("PROCESS FINISH: {}", rawRecord);
        filePipelineDirector.finishProcessing(rawRecord.getFilename());
        exchange.getIn().setBody(rawRecord.getFilename());
    }

    private void processSales(Exchange exchange) {
        RawRecord rawRecord = exchange.getIn().getBody(RawRecord.class);
        log.info("PROCESS SALES: {}", rawRecord);
        Sale sale = saleParser.parse(rawRecord.getLine());

        fileStats.processSales(sale);
        filePipelineDirector.processSales(rawRecord.getFilename(), sale);
    }

    private void processClient(Exchange exchange) {
        RawRecord rawRecord = exchange.getIn().getBody(RawRecord.class);
        log.info("PROCESS CLIENT: {}", rawRecord);
        Client client = clientParser.parse(rawRecord.getLine());

        fileStats.processClient(client);
        filePipelineDirector.addClient(rawRecord.getFilename(), client);
    }

    private void processSalesman(Exchange exchange) {
        RawRecord rawRecord = exchange.getIn().getBody(RawRecord.class);
        log.info("PROCESS SALESMAN: {}", rawRecord);
        Salesman salesman = salesmanParser.parse(rawRecord.getLine());

        fileStats.processSalesman(salesman);
        filePipelineDirector.addSalesman(rawRecord.getFilename(), salesman);
    }

    private static void unmarshall(Exchange exchange) throws IOException {
        RawRecord rawRecord = new ObjectMapper().readValue(exchange.getIn().getBody(String.class), RawRecord.class);

        if (rawRecord.getLine() != null && rawRecord.getLine().length() < 3) {
            exchange.getIn().setHeader("recordPrefix", "UNINDENTIFIABLE_RECORD");
            exchange.getIn().setBody(rawRecord);
            return;
        }

        String recordPrefix = rawRecord.getLine().substring(0,3);

        exchange.getIn().setHeader("recordPrefix", recordPrefix);
        exchange.getIn().setBody(rawRecord);
    }

    private void delayMinutes(int timeout) {
        try {
            TimeUnit.MINUTES.sleep(timeout);
        } catch (InterruptedException ignored) {
            //
        }
    }
}
