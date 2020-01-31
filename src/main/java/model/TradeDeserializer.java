package model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

public class TradeDeserializer implements Deserializer<Trade> {
    @Override
    public Trade deserialize(String s, byte[] bytes) {
        ObjectMapper mapper = new ObjectMapper();
        Trade trade = null;
        try {
            trade = mapper.readValue(bytes, Trade.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return trade;
    }
}
