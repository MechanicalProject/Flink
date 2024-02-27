import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
//import consumer.BookTickerConsumer;
import consumer.PriceConsumer;


public class FlinkApp{
    public static void main(String[] args){
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        PriceConsumer priceconsumer = new PriceConsumer(env, 1);
        priceconsumer.run();
        // bookticker_consumer.run();
        //BookTickerConsumer bookticker_consumer = new BookTickerConsumer(env, 1);
    
}
}

