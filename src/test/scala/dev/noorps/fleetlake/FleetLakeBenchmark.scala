package dev.noorps.fleetlake

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object FleetLakeBenchmark {
  final case class RunResult(
      elapsedMillis: Long,
      uniqueEvents: Long,
      aggregateRows: Long,
      faultEvents: Long
  )

  private def kafkaRecords(spark: SparkSession, eventVolume: Long): DataFrame = {
    val uniqueEventCount = Math.max(1L, Math.round(eventVolume * 0.95))
    val baseEpochSeconds = 1789221600L

    spark.range(eventVolume).select(
      to_json(
        struct(
          concat(lit("fleet-"), (col("id") % 8).cast("string")).as("tenantId"),
          concat(lit("device-"), (col("id") % 1000).cast("string")).as("deviceId"),
          concat(lit("event-"), (col("id") % uniqueEventCount).cast("string")).as("eventId"),
          date_format(
            to_timestamp(from_unixtime(lit(baseEpochSeconds) + (col("id") % 3600))),
            "yyyy-MM-dd'T'HH:mm:ss'Z'"
          ).as("recordedAt"),
          when(col("id") % 100 === 0, lit("fault_brake"))
            .otherwise(lit("battery_pct"))
            .as("metric"),
          (col("id") % 101).cast("double").as("value"),
          when(col("id") % 100 === 0, lit("critical"))
            .when(col("id") % 25 === 0, lit("warning"))
            .otherwise(lit("normal"))
            .as("severity")
        )
      ).cast("binary").as("value")
    )
  }

  private def execute(spark: SparkSession, eventVolume: Long): RunResult = {
    val startedAt = System.nanoTime()
    val events = FleetLakeJob
      .parseEvents(kafkaRecords(spark, eventVolume))
      .persist(StorageLevel.MEMORY_AND_DISK)

    val uniqueEvents = events.count()
    val aggregateRows = FleetLakeJob.minuteAggregates(events).count()
    val faultEvents = events
      .filter(col("metric").startsWith("fault_") || lower(col("severity")).isin("warning", "critical"))
      .count()

    events.unpersist(blocking = true)
    RunResult(
      elapsedMillis = Math.round((System.nanoTime() - startedAt) / 1000000.0),
      uniqueEvents = uniqueEvents,
      aggregateRows = aggregateRows,
      faultEvents = faultEvents
    )
  }

  private def percentile(values: Seq[Long], percentile: Double): Long = {
    val sorted = values.sorted
    val index = Math.max(0, Math.ceil(percentile * sorted.size).toInt - 1)
    sorted(index)
  }

  def main(args: Array[String]): Unit = {
    val eventVolume = args.headOption.map(_.toLong).getOrElse(200000L)
    val warmupRuns = args.lift(1).map(_.toInt).getOrElse(2)
    val measuredRuns = args.lift(2).map(_.toInt).getOrElse(5)

    require(eventVolume > 0, "event volume must be positive")
    require(warmupRuns >= 0, "warmup runs cannot be negative")
    require(measuredRuns >= 3, "use at least three measured runs")

    val spark = SparkSession.builder()
      .appName("fleetlake-benchmark")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "8")
      .config("spark.driver.memory", "2g")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    try {
      (1 to warmupRuns).foreach { run =>
        val result = execute(spark, eventVolume)
        println(s"warmup $run: ${result.elapsedMillis} ms")
      }

      val results = (1 to measuredRuns).map { run =>
        val result = execute(spark, eventVolume)
        println(s"measured $run: ${result.elapsedMillis} ms")
        result
      }

      val durations = results.map(_.elapsedMillis)
      val medianMillis = percentile(durations, 0.50)
      val p95Millis = percentile(durations, 0.95)
      val medianEventsPerSecond = Math.round(eventVolume * 1000.0 / medianMillis)
      val sample = results.head
      val javaVersion = System.getProperty("java.version")
      val processors = Runtime.getRuntime.availableProcessors()

      Files.createDirectories(Paths.get("benchmarks"))
      val json =
        s"""{
           |  "eventVolume": $eventVolume,
           |  "warmupRuns": $warmupRuns,
           |  "measuredRuns": $measuredRuns,
           |  "durationsMillis": [${durations.mkString(", ")}],
           |  "medianBatchMillis": $medianMillis,
           |  "p95BatchMillis": $p95Millis,
           |  "medianEventsPerSecond": $medianEventsPerSecond,
           |  "uniqueEvents": ${sample.uniqueEvents},
           |  "aggregateRows": ${sample.aggregateRows},
           |  "faultEvents": ${sample.faultEvents},
           |  "sparkVersion": "${spark.version}",
           |  "javaVersion": "$javaVersion",
           |  "availableProcessors": $processors,
           |  "mode": "local[*]",
           |  "scope": "parse, watermark, deduplicate, cache, aggregate, and fault-filter transforms; no external sink I/O"
           |}
           |""".stripMargin

      Files.write(Paths.get("benchmarks", "latest.json"), json.getBytes(StandardCharsets.UTF_8))
      println(json)
    } finally {
      spark.stop()
    }
  }
}
