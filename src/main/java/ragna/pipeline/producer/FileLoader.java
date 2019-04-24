package ragna.pipeline.producer;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ragna.pipeline.common.model.RawRecord;
import ragna.pipeline.director.FilePipelineDirector;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

@Component
@Slf4j
public class FileLoader extends RouteBuilder {

    @Autowired
    FilePipelineDirector filePipelineDirector;

    @Value("#{systemProperties['user.home']}")
    private String userHome;

    @Override
    public void configure() throws Exception {
        from("file:{{user.home}}/data/in?include=.*\\.dat")
                .routeId("FileLoader route")
                .errorHandler(deadLetterChannel("kafka:producererrortopic?brokers=localhost:9092"))
                .process(this::initPipeline)
                .process(this::preProcessFileSplit)
                .split(body()
                        .tokenize("\r\n|\n|\r")).streaming()
                .process(this::marshall)
                .to("kafka:filetopic?brokers=localhost:9092")
                .log("${in.headers.CamelFileName} - ${in.body} ")
        ;
    }

    private void initPipeline(Exchange exchange) {
        filePipelineDirector.beginProcessing(exchange.getIn().getHeader("CamelFileName", String.class));
    }

    private void marshall(Exchange exchange) throws JsonProcessingException {
        RawRecord rawRecord = new RawRecord(exchange.getIn().getHeader("CamelFileName", String.class),
                exchange.getIn().getBody(String.class));

        exchange.getIn().setBody(new ObjectMapper().writeValueAsString(rawRecord));
        log.info("Unmarshalled: {}", rawRecord);
    }

    private void preProcessFileSplit(Exchange exchange) throws IOException {
        addEnFileMarker(exchange);
    }

    private void addEnFileMarker(Exchange exchange) throws IOException {
        final String endFileMarker = "004çSTREAMEND\r\n";
        final String fileName = String.format("%s/data/in/%s", userHome
                , exchange.getIn().getHeader("CamelFileName"));
        Files.write(
                Paths.get(fileName),
                endFileMarker.getBytes(),
                StandardOpenOption.APPEND);
    }
}
