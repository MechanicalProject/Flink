package consumer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.core.fs.Path;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.Configuration;
import java.util.Properties;
import java.util.LinkedList;
import java.util.Collections;
import schema.Data;
import schema.Price;
import schema.KafkaPriceSchema;

import java.util.concurrent.TimeUnit;

public class PriceConsumer{
    
    private int parallelism;
    private StreamExecutionEnvironment env;
    private Properties properties;


    public PriceConsumer(StreamExecutionEnvironment env , int parallelism){
        this.parallelism = parallelism;
        this.env = env;
    }
    public void run(){
        KafkaSource kafkaSource = KafkaSource.<Price>builder()
            .setBootstrapServers("localhost:29092")
            .setTopics("price")
            .setGroupId("price-consumer-group")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new KafkaPriceSchema())
            .build();

        // api 바뀜 
        DataStream<Price> stream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // {"data": [{"symbol": "ETHBTC","price": "0.05428000"},{"symbol": "ADABTC","price": "0.00001158"}],"timestamp": "2024-02-16T13:42:55"}
        
        // symbol , price만 가져옴 . 개별 레코드로 변경 
        DataStream<Data> dataStream = stream.flatMap(new FlatMapFunction<Price, Data>() {
            @Override
            public void flatMap(Price price, Collector<Data> out) {
                for (Data data : price.data) {
                    out.collect(data); 
                }
            }
        });

        // Data 스트림을 'symbol'에 따라 분류
        // arima(1,1,0) 1,4,2,3,5 라면?
        // 차분 = 3, -2 , -1 , 2
        // weight= 0.1,0.2,0.3,0.4 최근이 높게 
        // 3 * 0.1 + (-2)* 0.2 + (-1) * 0.3 + 2 * 0.4 + 5 = 5.4   
        
        KeyedStream<Data, String> keyedStream = dataStream.keyBy(data -> data.symbol);

        keyedStream.process(new KeyedProcessFunction<String, Data, String>() {
            // Key(코인)별로 ListState 둠. [1,2,3,4] 이전 비트코인가격 저장위해 .
            // Rich function 정의 
            private transient ListState<Double> priceState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ListStateDescriptor<Double> descriptor = new ListStateDescriptor<>(
                    "priceState",
                    Double.class
                );
                priceState = getRuntimeContext().getListState(descriptor);
            }

            @Override
            public void processElement(Data value, Context ctx, Collector<String> out) throws Exception {
                
                // state에 저장해둔 List 가져와서 아까 수식대로 계산 
                LinkedList<Double> priceList = new LinkedList<>();
                for (Double price : priceState.get()){
                    priceList.add(price);
                }
                if (priceList.size() >= 5) {
                    priceList.removeFirst();
                }
                priceList.add(value.price);
                priceState.update(priceList);

                double weightedSum = 0;
                double[] weights = {0.1,0.2,0.3,0.4};
                double weightedPrice;
                int priceListLength =  priceList.size();
                if (priceListLength == 1 ){
                    weightedPrice = priceList.getFirst();
                }
                else{
                for (int i =1 ; i<priceListLength;i++){
                    double diff = priceList.get(i) - priceList.get(i-1);
                    weightedSum += diff * weights[i-1];
                }
                weightedPrice = weightedSum + priceList.getLast();
                }
                String result = value.symbol + "," + value.price + "," + weightedPrice;
                out.collect(result); // Fixed missing semicolon
            }
        })
                .addSink(
                    //결과 파일로 출력. 
                        StreamingFileSink
                                .forRowFormat(new Path("/Users/jungeui/Documents/workspace/Flink-Study/result/"), new SimpleStringEncoder<String>("UTF-8"))
                                .withRollingPolicy(DefaultRollingPolicy.builder()
                                        //1분마다 파일씀 
                                        .withRolloverInterval(TimeUnit.MINUTES.toMillis(1))
                                        //1분동안 아무것도 도착안하면 새파일 씀
                                        .withInactivityInterval(TimeUnit.MINUTES.toMillis(1))
                                        //파일최대크기 1gb
                                        .withMaxPartSize(1024 * 1024 * 1024)
                                        .build())
                                .build()
                ).setParallelism(parallelism);

        try {
            env.execute("Run Price Consumer");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}