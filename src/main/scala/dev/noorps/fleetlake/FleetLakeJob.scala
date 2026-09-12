package dev.noorps.fleetlake

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object FleetLakeJob {
  private val telemetrySchema = new StructType()
    .add("tenantId", StringType, nullable = false)
    .add("deviceId", StringType, nullable = false)
    .add("eventId", StringType, nullable = false)
    .add("recordedAt", TimestampType, nullable = false)
    .add("metric", StringType, nullable = false)
    .add("value", DoubleType, nullable = false)
    .add("severity", StringType, nullable = true)

  def parseEvents(kafkaRecords: DataFrame): DataFrame =
    kafkaRecords
      .select(from_json(col("value").cast("string"), telemetrySchema).as("event"))
      .select("event.*")
      .filter(col("tenantId").isNotNull && col("deviceId").isNotNull && col("eventId").isNotNull)
      .withWatermark("recordedAt", "2 minutes")
      .dropDuplicates("tenantId", "eventId")

  def minuteAggregates(events: DataFrame): DataFrame =
    events
      .groupBy(
        col("tenantId"),
        col("deviceId"),
        col("metric"),
        window(col("recordedAt"), "1 minute")
      )
      .agg(
        count(lit(1)).as("readingCount"),
        avg(col("value")).as("averageValue"),
        min(col("value")).as("minimumValue"),
        max(col("value")).as("maximumValue")
      )

  def main(args: Array[String]): Unit = {
    val kafkaServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    val topic = sys.env.getOrElse("KAFKA_TOPIC", "telemetry.events.v1")
    val lakePath = sys.env.getOrElse("LAKE_PATH", "data/telemetry")
    val checkpointRoot = sys.env.getOrElse("CHECKPOINT_PATH", "checkpoints")

    val spark = SparkSession.builder()
      .appName("fleetlake")
      .getOrCreate()

    val kafkaRecords = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServers)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()

    val events = parseEvents(kafkaRecords)

    events.writeStream
      .format("parquet")
      .partitionBy("tenantId", "metric")
      .option("path", lakePath)
      .option("checkpointLocation", s"$checkpointRoot/raw")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    minuteAggregates(events).writeStream
      .format("json")
      .outputMode("append")
      .option("path", s"$lakePath/aggregates")
      .option("checkpointLocation", s"$checkpointRoot/aggregates")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    events.filter(col("metric").startsWith("fault_") || lower(col("severity")).isin("warning", "critical"))
      .select(to_json(struct(col("*"))).as("value"))
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServers)
      .option("topic", "telemetry.faults.v1")
      .option("checkpointLocation", s"$checkpointRoot/faults")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    spark.streams.awaitAnyTermination()
  }
}
