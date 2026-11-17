-- flink sql: event-time tumbling window over the campus-events topic.
-- the kafka connector jar must match the installed flink version.

CREATE TABLE campus_events (
    eventId INT,
    studentId STRING,
    eventType STRING,
    department STRING,
    eventTime TIMESTAMP(3),
    WATERMARK FOR eventTime AS eventTime - INTERVAL '30' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'campus-events',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'tp06-flink',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

SELECT
    window_start,
    window_end,
    department,
    COUNT(*) AS checkins
FROM TABLE(
    TUMBLE(
        TABLE campus_events,
        DESCRIPTOR(eventTime),
        INTERVAL '1' MINUTE
    )
)
WHERE eventType = 'check-in'
GROUP BY window_start, window_end, department;
