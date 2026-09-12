SELECT
  tenant_id,
  device_id,
  count(*) AS readings,
  avg(value) AS average_value,
  max(_timestamp) AS latest_kafka_timestamp
FROM kafka.telemetry.events
WHERE metric = 'battery_pct'
GROUP BY tenant_id, device_id
ORDER BY latest_kafka_timestamp DESC;
