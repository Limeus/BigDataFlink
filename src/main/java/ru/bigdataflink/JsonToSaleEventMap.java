package ru.bigdataflink;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

public class JsonToSaleEventMap extends RichMapFunction<String, SaleEvent> {
    private transient ObjectMapper objectMapper;

    @Override
    public void open(Configuration parameters) {
        objectMapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    public SaleEvent map(String value) throws Exception {
        return objectMapper.readValue(value, SaleEvent.class);
    }
}
