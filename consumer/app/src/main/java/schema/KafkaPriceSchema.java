package schema;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;

public class KafkaPriceSchema extends AbstractDeserializationSchema<Price> {

    private static final long serialVersionUID = 1L;

    private transient ObjectMapper objectMapper;

    @Override
    public void open(InitializationContext context) {
        objectMapper = new ObjectMapper();
    }

    @Override
    public Price deserialize(byte[] message) throws IOException {
        return objectMapper.readValue(message, Price.class);
    }
}
