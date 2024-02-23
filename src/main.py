from pathlib import Path
from pyflink.common import Types
from pyflink.datastream import DataStream

from src.kafkaresolver import KafkaToKafkaStreamResolver
from src.event import PreprocessEvent
import json

class DataStreamResolver(KafkaToKafkaStreamResolver):
    def __init__(
        self,
        job_name: str,
        source_topic: str,
        consumer_group_id: str,
        target_topic: str,
        **kwargs,
    ):
        super().__init__(
            job_name=job_name,
            source_topic=source_topic,
            consumer_group_id=consumer_group_id,
            target_topic=target_topic,
            **kwargs,
        )

    def datastream_processing(self) -> DataStream:
        keyed_stream = (
            self.datastream.key_by(
                lambda x: eval(json.loads(x))['Symbol'],
                key_type=Types.STRING(),
            )
            .process(
                PreprocessEvent(), output_type=Types.STRING()
            )
            .set_parallelism(2)
        )

        return keyed_stream

if __name__ == "__main__":
    _job_name = Path(__file__).stem
    elff_datastream_resolver = DataStreamResolver(
        job_name=_job_name,
        source_topic="source",
        consumer_group_id="group_01",
        target_topic="target"
    )

    elff_datastream_resolver.resolve()