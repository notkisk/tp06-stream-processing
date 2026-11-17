# tp06 - real-time data processing using apache spark

simple lab project for the big data analytics module.

the project reads synthetic campus check-in events from kafka, processes them with spark structured streaming, counts check-ins by department in event-time windows, and prints late check-ins after 08:15 utc.

## files

```
tp06-stream-processing/
├── producer.py
├── spark_stream.py
├── requirements.txt
├── flink_window.sql
├── report.md
├── README.md
└── screenshots/
```

## versions used

- python 3.11
- pyspark 3.5.3
- scala 2.12
- spark kafka connector `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3`
- kafka 3.7.1 (kraft mode, localhost:9092)
- java 17 (temurin)
- flink sql: query analysis only, not executed (no flink cluster was used)

## setup

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

spark needs java 17:

```bash
export JAVA_HOME=/path/to/java-17
export PATH="$JAVA_HOME/bin:$PATH"
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PWD/.venv/bin/python"
```

## start kafka

start the kafka broker from tp05 if it is already available. otherwise start a local kafka 3.7.1 broker in kraft mode and create the topic:

```bash
kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic campus-events \
  --partitions 3 --replication-factor 1

kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic campus-events
```

## run the producer

```bash
python producer.py
```

use `--morning` to set the base time to 08:14:30 utc, so that `S411` and `S204` are after 08:15:

```bash
python producer.py --morning
```

## run spark

start spark first, then run the producer in a second terminal.

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  --conf spark.ui.enabled=false \
  spark_stream.py --trigger 5
```

## options

| option | default | meaning |
|---|---|---|
| `--trigger` | `0` | processing-time trigger in seconds, `0` = default |
| `--window` | `1 minute` | window duration |
| `--slide` | empty | window slide, empty = tumbling |
| `--watermark` | `30 seconds` | watermark delay |
| `--checkpoint` | `./checkpoint/campus-events` | checkpoint base directory |

## experiments

trigger interval:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  spark_stream.py --trigger 5
```

2 minute tumbling window:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  spark_stream.py --window "2 minutes" --checkpoint ./checkpoint/two_min
```

2 minute window with 1 minute slide:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  spark_stream.py --window "2 minutes" --slide "1 minute" \
  --checkpoint ./checkpoint/sliding
```

2 minute watermark:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  spark_stream.py --watermark "2 minutes" --checkpoint ./checkpoint/wm2m
```

checkpoint recovery: stop spark with `ctrl+c`, keep `./checkpoint/recovery`, then start again with the same `--checkpoint ./checkpoint/recovery`.

## screenshots

- `kafka_topic.png` - topic created with 3 partitions
- `producer_output.png` - producer sending 7 events
- `spark_output.png` - counts by department
- `spark_counts_initial.png` - first count table
- `late_checkins.png` - `S411` and `S204` after 08:15
- `window_sliding.png` - overlapping 2 minute / 1 minute windows
- `window_2min_tumbling.png` - 2 minute tumbling window
- `checkpoint_or_recovery.png` - checkpoint folder
- `spark_startup.png` - spark startup log

## report

see `report.md`.
