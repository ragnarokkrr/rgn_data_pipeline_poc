package ragna.pipeline.reporter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ragna.pipeline.director.FilePipelineDirector;
import ragna.pipeline.director.model.ReportResults;

@Component
@Slf4j
public class Reporter extends RouteBuilder {
    @Autowired
    private FilePipelineDirector filePipelineDirector;
    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure() throws Exception {
        from("kafka:reportertopic?brokers=localhost:9092&groupId=Reporter")
                .routeId("Reporter route")
                .process(this::downloadReport)
                .process(this::format)
                .to("file:{{user.home}}/data/out");
    }

    private void format(Exchange exchange) throws JsonProcessingException {
        Object body = exchange.getIn().getBody();
        exchange.getIn().setBody(objectMapper.writeValueAsString(body));
    }

    private void downloadReport(Exchange exchange) {
        String fileName = exchange.getIn().getBody(String.class);
        log.info("DOWNLOADING {}", fileName);
        ReportResults reportResults = filePipelineDirector.downloadReport(fileName);
        String outputFileName = fileName.replaceAll("\\.dat", ".done.dat");
        exchange.getIn().setHeader("CamelFileName", outputFileName);
        exchange.getIn().setBody(reportResults);
    }
}
