package ragna.pipeline.director.converter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;
import ragna.pipeline.common.model.Sale;

/**
 * Uuuhhh, Convert?
 * TODO Fix Converter Registering in RedisTemplate: {@link ragna.pipeline.director.config.RedisConfiguration}.
 */
@Component
@Slf4j
public class SalesConverter implements Converter<Sale, String> {
    private ObjectMapper objectMapper = new ObjectMapper();
    @Override
    public String convert(Sale source) {
        try {
            return objectMapper.writeValueAsString(source);
        } catch (JsonProcessingException e) {
            log.debug("Sale ConversionError: {}", source);
            return "{}";
        }
    }
}
