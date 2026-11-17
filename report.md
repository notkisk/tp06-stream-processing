# tp06 report - real-time data processing using apache spark

## 1. what was built

a kafka producer sends seven synthetic campus check-in events to the topic `campus-events`. a spark structured streaming application reads the topic, parses the json, keeps only `check-in` events, and counts them by department in one minute event-time windows. a second query prints check-ins that happen after 08:15:00 utc.

the main files are `producer.py`, `spark_stream.py`, `flink_window.sql`, and the screenshots folder.

## 2. configuration used

- spark 3.5.3, scala 2.12
- kafka connector `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3`
- kafka 3.7.1 in kraft mode on `localhost:9092`
- java 17, python 3.11
- topic: `campus-events`, 3 partitions, replication factor 1
- trigger: 5 seconds for the screenshots
- watermark: 30 seconds
- checkpoint locations: `./checkpoint/campus-events/counts` and `./checkpoint/campus-events/late`

the spark session timezone is set to `utc` because the producer writes utc timestamps.

## 3. task 4 experiments

### experiment a - trigger

with `.trigger(processingTime='5 seconds')`, spark starts a micro-batch every five seconds. this is not the same as handling each event five seconds after it arrives. one batch can contain many events, and the trigger only controls when a batch starts.

### experiment b - window size

with a 2 minute tumbling window, the seven events fall into fewer groups. the output had three rows:

```
{08:14:00, 08:16:00} computer science 2
{08:14:00, 08:16:00} physics         2
{08:14:00, 08:16:00} mathematics     2
```

### experiment c - sliding window

with `window('event_ts', '2 minutes', '1 minute')`, one event can belong to two windows because the windows overlap by one minute. the screenshot `window_sliding.png` shows eight rows instead of three.

### experiment d - watermark

a larger watermark such as 2 minutes tells spark to wait longer for late events. this can keep more late events, but final window results appear later and state is kept longer. a small watermark gives faster results but drops more late events.

### experiment e - recovery

the application was stopped with `ctrl+c` and restarted with the same checkpoint directory. spark restored the committed kafka offsets and the window state. batch numbers continued instead of starting again, and old events were not replayed. the checkpoint directory must stay in the same place because it stores offsets, commits, and state files.

## 4. task 5 - late check-ins

the rule is: a check-in is late when its utc event time is after `08:15:00`.

the query compares the full event timestamp with a cutoff built from the event date and `08:15:00`, so `08:15:18` is correctly detected as late.

results from the run:

```
S411 computer science 2026-11-17T08:15:05+00:00
S204 mathematics      2026-11-17T08:15:18+00:00
```

event time is better than consumer arrival time because network delays, retries, and out-of-order delivery can change arrival time, but they do not change when the student really checked in.

## 5. task 6 - flink sql questions

1. the event-time field is `eventTime`.
2. `WATERMARK FOR eventTime AS eventTime - INTERVAL '30' SECOND` means flink expects events up to 30 seconds late. it uses this to decide when a window is complete.
3. each tumbling window is 1 minute long.
4. `window_start` and `window_end` show the exact time range of each result. this makes the output easy to read, sort, and send to a dashboard.

the flink query is in `flink_window.sql`. it was analysed but not executed because no flink cluster was used.

## 6. report questions

### 1. kafka role vs spark/flink role

kafka transports and stores events. spark or flink reads those events and computes results. kafka does not calculate window counts by itself.

### 2. how spark structured streaming represents a stream

spark treats the stream as an unbounded table. every micro-batch adds new rows, and spark updates the result table incrementally.

### 3. what is a micro-batch

a micro-batch is a small group of events processed together at a trigger time.
advantage: good throughput and simple recovery.
trade-off: results are not truly per-event real time, and latency depends on the trigger interval.

### 4. startingOffsets and subscribe

`subscribe` selects the kafka topic to read. `startingOffsets` decides where to start when there is no saved checkpoint, for example `earliest` or `latest`.

### 5. why define the json schema

spark needs the field names and types to parse json correctly. without an explicit schema, bad or missing fields are harder to detect and spark may guess types.

### 6. event time vs processing time

event time is when the event happened. processing time is when spark processes it. example: `S512` has event time `08:14:42`, but spark receives it after events with later event times. the window uses `08:14:42`, not the arrival time.

### 7. what is a watermark

a watermark is a moving threshold for how late an event may be. it does not guarantee that no late event is lost. events older than the watermark can be dropped.

### 8. tumbling, sliding, session windows

- tumbling: fixed size, no overlap, each event in one window.
- sliding: fixed size, overlapping, one event can be in several windows.
- session: groups events that are close in time and closes after a gap with no events.

### 9. what a checkpoint preserves

a checkpoint stores kafka offsets, query metadata, commits, and aggregate state. its location must stay stable because a restart reads that same state. moving or deleting it loses the recovery point.

### 10. two differences between spark and flink

- spark structured streaming normally uses micro-batches, while flink is a native streaming engine with low latency.
- flink has built-in event-time watermarks and stateful operators in sql, while spark streaming also supports event time but is tied to the micro-batch model.

### 11. which engine for this campus scenario

spark is a good choice here because the team already uses kafka and pyspark, one minute windows do not need very low latency, and the ecosystem is familiar. flink would be better if the system needed very low latency or more advanced event-time state handling.

### 12. screenshots

the screenshots are in `screenshots/`:

- kafka topic with 3 partitions
- producer output
- spark counts by department
- late check-ins after 08:15
- sliding window experiment
- 2 minute tumbling window
- checkpoint folders

## 7. problems and fixes

- the container from tp05 was not available in this environment, so kafka 3.7.1 was started locally in kraft mode at `localhost:9092`.
- spark initially used the local timezone, so 08:14 utc appeared as 09:14 and every check-in looked late. the fix was `spark.sql.session.timeZone=utc`.
- `spark-submit` first used the system python instead of the virtual environment. the fix was to set `PYSPARK_PYTHON` and `PYSPARK_DRIVER_PYTHON` to `.venv/bin/python`.
- the late rule first compared only the minute field, so `08:15:18` was missed. the fix was to compare the full timestamp with `08:15:00`.
