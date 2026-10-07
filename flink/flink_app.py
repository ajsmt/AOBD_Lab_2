import json
import time
from pyflink.common import WatermarkStrategy, Duration, Time, Types
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.window import (
    TumblingEventTimeWindows,
    SlidingEventTimeWindows,
    EventTimeSessionWindows,
)
from pyflink.datastream.functions import ProcessWindowFunction

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

env.add_jars("file:///opt/flink/usrlib/flink-sql-connector-kafka-3.0.2-1.18.jar")

kafka_source = (
    KafkaSource.builder()
    .set_bootstrap_servers("kafka:29092")
    .set_topics("events-topic")
    .set_group_id("flink-group")
    .set_starting_offsets(KafkaOffsetsInitializer.latest())
    .set_value_only_deserializer(SimpleStringSchema())
    .build()
)


def parse_json(msg_str):
    try:
        return json.loads(msg_str)
    except:
        return None


raw_stream = (
    env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), "KafkaSource")
    .map(parse_json)
    .filter(lambda x: x is not None)
)


class EventTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        return int(value["timestamp_ms"])


watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(
    Duration.of_seconds(10)
).with_timestamp_assigner(EventTimestampAssigner())

stream = raw_stream.assign_timestamps_and_watermarks(watermark_strategy)


class TumblingWindowProcess(ProcessWindowFunction):
    def process(self, key, context, elements):
        count = len(list(elements))
        start_str = time.strftime(
            "%H:%M:%S", time.localtime(context.window().start / 1000)
        )
        end_str = time.strftime("%H:%M:%S", time.localtime(context.window().end / 1000))
        yield f"[4b_FLINK_TUMBLING] window={start_str}-{end_str} action={key} count={count}"


stream.key_by(lambda x: x["action"]).window(
    TumblingEventTimeWindows.of(Time.seconds(10))
).process(TumblingWindowProcess()).print()


class SlidingWindowProcess(ProcessWindowFunction):
    def process(self, key, context, elements):
        count = len(list(elements))
        start_str = time.strftime(
            "%H:%M:%S", time.localtime(context.window().start / 1000)
        )
        end_str = time.strftime("%H:%M:%S", time.localtime(context.window().end / 1000))
        yield f"[4b_FLINK_SLIDING] window={start_str}-{end_str} action={key} count={count}"


stream.key_by(lambda x: x["action"]).window(
    SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5))
).process(SlidingWindowProcess()).print()


class SessionWindowProcess(ProcessWindowFunction):
    def process(self, key, context, elements):
        count = len(list(elements))
        start_str = time.strftime(
            "%H:%M:%S", time.localtime(context.window().start / 1000)
        )
        end_str = time.strftime("%H:%M:%S", time.localtime(context.window().end / 1000))
        yield f"[4b_FLINK_SESSION] session={start_str}-{end_str} user_id={key} session_count={count}"


stream.key_by(lambda x: x["user_id"]).window(
    EventTimeSessionWindows.with_gap(Time.seconds(10))
).process(SessionWindowProcess()).print()


class Stateful1MinProcess(ProcessWindowFunction):
    def process(self, key, context, elements):
        count = len(list(elements))
        start_str = time.strftime(
            "%H:%M:%S", time.localtime(context.window().start / 1000)
        )
        end_str = time.strftime("%H:%M:%S", time.localtime(context.window().end / 1000))
        yield f"[4c_FLINK_STATEFUL_1MIN] window={start_str}-{end_str} user_id={key} events_per_minute={count}"


stream.key_by(lambda x: x["user_id"]).window(
    TumblingEventTimeWindows.of(Time.seconds(60))
).process(Stateful1MinProcess()).print()


class MetricsProcess(ProcessWindowFunction):
    def process(self, key, context, elements):
        items = list(elements)
        total = len(items)
        now_ms = int(time.time() * 1000)
        # Latency = время выполнения в воркере минус время отправки в продюсере
        latencies = [max(0, now_ms - x["timestamp_ms"]) for x in items]
        avg_latency = round(sum(latencies) / total, 2) if total > 0 else 0
        throughput = round(total / 10.0, 2)
        start_str = time.strftime(
            "%H:%M:%S", time.localtime(context.window().start / 1000)
        )
        end_str = time.strftime("%H:%M:%S", time.localtime(context.window().end / 1000))
        yield f"[4d_FLINK_METRICS] window={start_str}-{end_str} total={total} avg_latency={avg_latency}ms throughput={throughput}rec/s"


stream.key_by(lambda x: "all").window(
    TumblingEventTimeWindows.of(Time.seconds(10))
).process(MetricsProcess()).print()

env.execute("Flink_Streaming_Full_Pipeline")
