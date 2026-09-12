package dev.noorps.fleetlake

final case class TelemetryEvent(
    tenantId: String,
    deviceId: String,
    eventId: String,
    recordedAt: String,
    metric: String,
    value: Double,
    severity: Option[String]
)

object TelemetryEvent {
  def partitionKey(tenantId: String, deviceId: String): String =
    s"$tenantId:$deviceId"

  def isFault(metric: String, severity: Option[String]): Boolean =
    metric.startsWith("fault_") || severity.exists(level => Set("warning", "critical").contains(level.toLowerCase))
}
