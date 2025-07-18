-- Create keyspace for IoT fleet monitoring
CREATE KEYSPACE IF NOT EXISTS fleet_monitoring 
WITH REPLICATION = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

USE fleet_monitoring;

-- Raw vehicle telemetry data
CREATE TABLE IF NOT EXISTS vehicle_telemetry (
    vehicle_id text,
    timestamp timestamp,
    latitude double,
    longitude double,
    speed double,
    engine_temp double,
    fuel_level double,
    PRIMARY KEY (vehicle_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);

-- Aggregated vehicle metrics (hourly aggregates)
CREATE TABLE IF NOT EXISTS vehicle_metrics_hourly (
    vehicle_id text,
    hour timestamp,
    avg_speed double,
    max_speed double,
    min_speed double,
    avg_engine_temp double,
    max_engine_temp double,
    avg_fuel_level double,
    min_fuel_level double,
    distance_traveled double,
    total_records bigint,
    PRIMARY KEY (vehicle_id, hour)
) WITH CLUSTERING ORDER BY (hour DESC);

-- Vehicle alerts and anomalies
CREATE TABLE IF NOT EXISTS vehicle_alerts (
    alert_id uuid,
    vehicle_id text,
    alert_type text,
    alert_message text,
    severity text,
    timestamp timestamp,
    resolved boolean,
    PRIMARY KEY (vehicle_id, timestamp, alert_id)
) WITH CLUSTERING ORDER BY (timestamp DESC);

-- Vehicle fleet summary
CREATE TABLE IF NOT EXISTS fleet_summary (
    summary_date date,
    total_vehicles int,
    active_vehicles int,
    total_alerts int,
    avg_fleet_speed double,
    avg_fleet_fuel double,
    PRIMARY KEY (summary_date)
);

-- Create indexes for efficient querying
CREATE INDEX IF NOT EXISTS ON vehicle_alerts (alert_type);
CREATE INDEX IF NOT EXISTS ON vehicle_alerts (severity);
CREATE INDEX IF NOT EXISTS ON vehicle_telemetry (timestamp);
