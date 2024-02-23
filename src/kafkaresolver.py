from enum import Enum
from pyflink.common import WatermarkStrategy, Configuration
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.state_backend import EmbeddedRocksDBStateBackend
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
    KafkaOffsetResetStrategy,
)
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.data_stream import DataStream

from src.streamresolver import StreamResolver

class StartingOffset(str, Enum):
    Earliest = "earliest"
    Latest = "latest"
    CommittedOffsets = "committed_offsets"

    def __str__(self) -> str:
        return str.__str__(self)


from pyflink.common.typeinfo import Types
from pyflink.datastream.connectors.kafka import FlinkKafkaProducer

class KafkaToKafkaStreamResolver(StreamResolver):
    def __init__(
        self,
        job_name: str,
        source_topic: str,
        consumer_group_id: str,
        target_topic: str,
    ):
        self._job_name = job_name
        self._source_topic = source_topic
        self._consumer_group_id = consumer_group_id
        self._target_topic = target_topic
        self._starting_offset = StartingOffset.CommittedOffsets

    def resolve(self):
        # flink run --python ~/src/main.py --pyFiles /opt/flink
        config = Configuration()
        config.set_string("python.executable", "/opt/flink/venv/bin/python3")
        env = StreamExecutionEnvironment.get_execution_environment(config)
        env.set_stream_time_characteristic(TimeCharacteristic.ProcessingTime)
        env.set_state_backend(EmbeddedRocksDBStateBackend())
        self._get_source_datastream(env)
        self._sink_to_kafka(self.datastream_processing())
        env.execute(self._job_name)

    def datastream_processing(self) -> DataStream:
        pass

    def _sink_to_kafka(self, keyed_stream: DataStream):
        kafka_producer = FlinkKafkaProducer(
            topic=self._target_topic,
            serialization_schema=SimpleStringSchema(),
            producer_config={
                "bootstrap.servers": "kafka:9092",
                "group.id": self._consumer_group_id,
            },
        )
        keyed_stream.add_sink(kafka_producer)

    def _get_source_datastream(self, env: StreamExecutionEnvironment):
        source = self._get_kafka_source()
        self.datastream = env.from_source(
            source=source,
            watermark_strategy=WatermarkStrategy.no_watermarks(),
            source_name=self._source_topic,
        ).set_parallelism(2)

    def _get_kafka_source(self):
        _starting_offset = self._get_starting_offset()
        source = (
            KafkaSource.builder()
            .set_bootstrap_servers("kafka:9092")
            .set_topics(self._source_topic)
            .set_group_id(self._consumer_group_id)
            .set_starting_offsets(_starting_offset)
            .set_value_only_deserializer(SimpleStringSchema())
            .build()
        )
        return source

    def _get_starting_offset(self):
        if self._starting_offset.__eq__(StartingOffset.Earliest):
            return KafkaOffsetsInitializer.earliest()
        elif self._starting_offset.__eq__(StartingOffset.Latest):
            return KafkaOffsetsInitializer.latest()
        elif self._starting_offset.__eq__(StartingOffset.CommittedOffsets):
            return KafkaOffsetsInitializer.committed_offsets(
                KafkaOffsetResetStrategy.LATEST)
        return KafkaOffsetsInitializer.committed_offsets(
            KafkaOffsetResetStrategy.LATEST)