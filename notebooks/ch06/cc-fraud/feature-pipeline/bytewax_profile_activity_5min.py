from io import BytesIO
from hsfs import engine

import statistics
import json
from datetime import datetime, timedelta, timezone

import bytewax.operators.window as win
from bytewax import operators as op
from bytewax.dataflow import Dataflow
from bytewax.connectors.kafka import KafkaSourceMessage, KafkaSinkMessage
from bytewax.connectors.kafka import operators as kop
from bytewax.operators.window import EventClockConfig, TumblingWindow, SlidingWindow

import hopsworks


def _get_feature_group_config(feature_group):
    """
    fetches configuration for feature group online topic
    :param feature_group:
    :return:
    """

    if feature_group._kafka_producer is None:
        offline_write_options = {}
        producer, feature_writers, writer = engine.get_instance()._init_kafka_resources(
            feature_group, offline_write_options
        )
        feature_group._kafka_producer = producer
        feature_group._feature_writers = feature_writers
        feature_group._writer = writer

    return feature_group


def serialize_with_key(key_payload, feature_group):
    key, row = key_payload

    feature_group = _get_feature_group_config(feature_group)

    # encode complex features
    row = engine.get_instance()._encode_complex_features(
        feature_group._feature_writers, row
    )

    # encode feature row
    with BytesIO() as outf:
        feature_group._writer(row, outf)
        encoded_row = outf.getvalue()

    # assemble key
    key = "".join([str(row[pk]) for pk in sorted(feature_group.primary_key)])

    return key, encoded_row


def sink_kafka(key, value, feature_group):  # -> KafkaSinkMessage[Dict, Dict]:

    # encode complex features
    headers = [
        ("projectId", str(feature_group.feature_store.project_id).encode("utf8")),
        ("featureGroupId", str(feature_group._id).encode("utf8")),
        ("subjectId", str(feature_group.subject["id"]).encode("utf8")),
    ]

    return KafkaSinkMessage(
        headers=headers,  # List[Tuple[str, bytes]] = field(default_factory=list)
        key=str({"identifier": key, "name": feature_group._online_topic_name}).encode(
            "utf-8"
        ),
        value=value,
    )


def get_kafka_config(feature_store_id):
    return engine.get_instance()._get_kafka_config(feature_store_id)


# This is the accumulator function, and outputs a list of 2-tuples,
# containing the event's "value" and it's "time" (used later to print info)
def accumulate(acc, event):
    acc.append((event["amount"], event["datetime"]))
    return acc


# This function instructs the event clock on how to retrieve the
# event's datetime from the input.
# Note that the datetime MUST be UTC. If the datetime is using a different
# representation, we would have to convert it here.
def get_event_time(event):
    return datetime.strptime(event["datetime"], "%Y-%m-%d %H:%M:%S").replace(
        tzinfo=timezone.utc
    )


def format_event(event):
    key, (metadata, data) = event
    values = [x[0] for x in data]
    timestamp = (
        int(
            datetime.strptime(data[-1][1], "%Y-%m-%d %H:%M:%S")
            .replace(tzinfo=timezone.utc)
            .timestamp()
        )
        * 1000
        * 1000
    ) # timestamp is in micro-second

    return key, {
        "account_id": key,
        "timestamp": timestamp,
        "min_amount": min(values),
        "max_amount": max(values),
        "count": len(values),
        "mean": statistics.mean(values),
    }


def get_flow():
    project = hopsworks.login()
    fs = project.get_feature_store()

    # get feature group and its topic configuration
    feature_group = fs.get_feature_group("profiles_activity_5m", 1)

    # get kafka connection config
    kafka_config = get_kafka_config(feature_store_id=fs.id)
    kafka_config["auto.offset.reset"] = "earliest"

    flow = Dataflow("windowing")
    align_to = datetime(2023, 1, 1, tzinfo=timezone.utc)

    # Define the dataflow object and kafka input.
    stream = kop.input(
        "kafka-in",
        flow,
        brokers=[kafka_config["bootstrap.servers"]],
        topics=["real_time_live_transactions"],
        add_config=kafka_config,
    )

    def parse_value(msg: KafkaSourceMessage):
        return json.loads(msg.value)

    parsed_stream = op.map("parse_value", stream.oks, parse_value)

    #######################################################
    # Group the readings by account, so that we only
    # aggregate readings of the same type.
    keyed_stream = op.key_on("key_on_user", parsed_stream, lambda e: e["cc_num"])

    # Configure the `fold_window` operator to use the event time.
    clock = EventClockConfig(
        get_event_time, wait_for_system_duration=timedelta(seconds=10)
    )

    windower = SlidingWindow(
        length=timedelta(minutes=5),
        offset=timedelta(minutes=1),
        align_to=align_to,
    )

    windowed_stream = win.fold_window(
        "add", keyed_stream, clock, windower, list, accumulate
    )

    formatted_stream = op.map("format_event", windowed_stream, format_event)

    # sync to feature group topic
    fg_serialized_stream = op.map(
        "serialize_with_key",
        formatted_stream,
        lambda x: serialize_with_key(x, feature_group),
    )

    processed = op.map(
        "map", fg_serialized_stream, lambda x: sink_kafka(x[0], x[1], feature_group)
    )

    kop.output(
        "kafka-out",
        processed,
        brokers=kafka_config["bootstrap.servers"],
        topic=feature_group._online_topic_name,
        add_config=kafka_config,
    )

    return flow
