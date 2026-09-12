package dev.noorps.fleetlake

import org.scalatest.funsuite.AnyFunSuite

class TelemetryEventSpec extends AnyFunSuite {
  test("partition keys isolate tenants and keep one device ordered") {
    assert(TelemetryEvent.partitionKey("fleet-a", "vehicle-7") == "fleet-a:vehicle-7")
    assert(TelemetryEvent.partitionKey("fleet-a", "vehicle-7") != TelemetryEvent.partitionKey("fleet-b", "vehicle-7"))
  }

  test("fault classification includes fault metrics and elevated severities") {
    assert(TelemetryEvent.isFault("fault_battery", None))
    assert(TelemetryEvent.isFault("temperature", Some("critical")))
    assert(!TelemetryEvent.isFault("battery_pct", Some("normal")))
  }
}
