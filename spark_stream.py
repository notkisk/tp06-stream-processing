import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat,
    date_format,
    from_json,
    lit,
    to_timestamp,
    window,
)
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def parse_args():
    parser = argparse.ArgumentParser(description="campus real-time analytics with spark structured streaming")
    parser.add_argument("--trigger", type=int, default=0, help="processing-time trigger in seconds (0 = default)")
    parser.add_argument("--window", default="1 minute", help="window duration, for example '1 minute'")
    parser.add_argument("--slide", default="", help="window slide, for example '1 minute' (empty = tumbling)")
    parser.add_argument("--watermark", default="30 seconds", help="watermark delay, for example '30 seconds'")
    parser.add_argument("--checkpoint", default="./checkpoint/campus-events", help="base checkpoint directory")
    return parser.parse_args()


def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("CampusRealTimeAnalytics")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("eventId", IntegerType(), True),
        StructField("studentId", StringType(), True),
        StructField("eventType", StringType(), True),
        StructField("department", StringType(), True),
        StructField("eventTime", StringType(), True),
    ])

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "campus-events")
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed = (
        raw.selectExpr("CAST(value AS STRING) AS json")
        .select(from_json(col("json"), schema).alias("e"))
        .select("e.*")
        .withColumn("event_ts", to_timestamp(col("eventTime")))
        .filter(col("event_ts").isNotNull())
    )

    checkins = parsed.filter(col("eventType") == "check-in")

    if args.slide:
        window_expr = window(col("event_ts"), args.window, args.slide)
    else:
        window_expr = window(col("event_ts"), args.window)

    counts = (
        checkins
        .withWatermark("event_ts", args.watermark)
        .groupBy(window_expr, col("department"))
        .count()
    )

    counts_query = counts.writeStream
    counts_query = (
        counts_query
        .outputMode("update")
        .format("console")
        .option("truncate", "false")
        .option("checkpointLocation", args.checkpoint + "/counts")
    )
    if args.trigger > 0:
        counts_query = counts_query.trigger(processingTime=f"{args.trigger} seconds")
    counts_query = counts_query.start()

    # task 5: a check-in is late when its utc event time is after 08:15:00.
    cutoff = to_timestamp(
        concat(date_format(col("event_ts"), "yyyy-MM-dd"), lit(" 08:15:00"))
    )
    late = checkins.filter(col("event_ts") > cutoff).select(
        "studentId", "department", "eventTime"
    )

    late_query = (
        late.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", "false")
        .option("checkpointLocation", args.checkpoint + "/late")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
