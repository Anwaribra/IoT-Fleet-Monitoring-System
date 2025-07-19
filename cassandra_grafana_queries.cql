
-- CASSANDRA GRAFANA QUERIES FOR IOT FLEET
-

-- 1. REAL-TIME FLEET OVERVIEW
-- 

-- Current Vehicle Status (for Table visualization)
SELECT vehicle_id, speed, engine_temp, fuel_level, latitude, longitude, timestamp
FROM fleet_monitoring.vehicle_telemetry
WHERE vehicle_id IN ('TRUCK_001', 'TRUCK_002', 'TRUCK_003', 'VAN_001', 'VAN_002', 'BUS_001', 'BIKE_001', 'BIKE_002', 'CAR_001', 'CAR_002')
AND timestamp > maxTimeuuid('2025-07-19 15:00:00')
ORDER BY timestamp DESC
LIMIT 50;

-- Vehicle Data for Specific Vehicle (Time Series)
SELECT timestamp, speed, engine_temp, fuel_level
FROM fleet_monitoring.vehicle_telemetry
WHERE vehicle_id = 'TRUCK_001'
AND timestamp > maxTimeuuid('2025-07-19 15:00:00')
ORDER BY timestamp DESC;

-- 2. FLEET PERFORMANCE METRICS
-- ============================

-- All Vehicles Recent Data (for aggregation in Grafana)
SELECT vehicle_id, speed, engine_temp, fuel_level, timestamp
FROM fleet_monitoring.vehicle_telemetry
WHERE vehicle_id IN ('TRUCK_001', 'TRUCK_002', 'TRUCK_003', 'VAN_001', 'VAN_002', 'BUS_001', 'BIKE_001', 'BIKE_002', 'CAR_001', 'CAR_002')
AND timestamp > maxTimeuuid('2025-07-19 15:00:00')
ORDER BY timestamp DESC;

-- High Temperature Alerts
SELECT vehicle_id, engine_temp, timestamp
FROM fleet_monitoring.vehicle_telemetry
WHERE vehicle_id IN ('TRUCK_001', 'TRUCK_002', 'TRUCK_003', 'VAN_001', 'VAN_002', 'BUS_001', 'BIKE_001', 'BIKE_002', 'CAR_001', 'CAR_002')
AND timestamp > maxTimeuuid('2025-07-19 15:00:00')
ORDER BY timestamp DESC;

-- Low Fuel Alerts
SELECT vehicle_id, fuel_level, timestamp
FROM fleet_monitoring.vehicle_telemetry
WHERE vehicle_id IN ('TRUCK_001', 'TRUCK_002', 'TRUCK_003', 'VAN_001', 'VAN_002', 'BUS_001', 'BIKE_001', 'BIKE_002', 'CAR_001', 'CAR_002')
AND timestamp > maxTimeuuid('2025-07-19 15:00:00')
ORDER BY timestamp DESC;

-- 3. GEOGRAPHIC DATA
-- ===========================
-- Vehicle Locations (for Geomap)
SELECT vehicle_id, latitude, longitude, speed, timestamp
FROM fleet_monitoring.vehicle_telemetry
WHERE vehicle_id IN ('TRUCK_001', 'TRUCK_002', 'TRUCK_003', 'VAN_001', 'VAN_002', 'BUS_001', 'BIKE_001', 'BIKE_002', 'CAR_001', 'CAR_002')
AND timestamp > maxTimeuuid('2025-07-19 16:00:00')
ORDER BY timestamp DESC
LIMIT 100;

-- Vehicle Track for Specific Vehicle
SELECT latitude, longitude, speed, timestamp
FROM fleet_monitoring.vehicle_telemetry
WHERE vehicle_id = 'TRUCK_001'
AND timestamp > maxTimeuuid('2025-07-19 15:00:00')
ORDER BY timestamp ASC;

-- ===============================================
-- GRAFANA SETUP INSTRUCTIONS


/*
CASSANDRA DATASOURCE SETUP:
1. Host: localhost:9042
2. Keyspace: fleet_monitoring
3. Consistency Level: ONE
4. Protocol Version: V4

VISUALIZATION TYPES TO USE:

 STAT PANELS:
- Use: COUNT() aggregations in Grafana
- Query: SELECT vehicle_id FROM vehicle_telemetry...
- Transform: Reduce with "Count" function

 TIME SERIES:
- Query: SELECT timestamp, speed, engine_temp FROM vehicle_telemetry WHERE vehicle_id = 'TRUCK_001'
- Time Field: timestamp
- Value Fields: speed, engine_temp

 GEOMAP:
- Query: SELECT latitude, longitude, vehicle_id FROM vehicle_telemetry...
- Latitude Field: latitude
- Longitude Field: longitude

TABLE:
- Query: SELECT vehicle_id, speed, engine_temp, fuel_level, timestamp FROM vehicle_telemetry...
- Show all columns

 HEATMAP:
- Query: SELECT vehicle_id, engine_temp FROM vehicle_telemetry...
- X-Axis: vehicle_id
- Y-Axis: engine_temp

VARIABLES FOR DYNAMIC DASHBOARDS:
- $vehicle_id: Custom variable with values (TRUCK_001, TRUCK_002, etc.)
- $time_range: Use Grafana's built-in time range

TRANSFORMATION FUNCTIONS:
- Group by: vehicle_id
- Reduce: Last, Mean, Max, Min
- Filter by value: engine_temp > 100, fuel_level < 20
*/

-- ===============================================
-- SAMPLE DASHBOARD PANELS
-- ===============================================

-- Panel 1: Active Vehicles Count (Stat)
SELECT COUNT(DISTINCT vehicle_id) as active_vehicles
FROM fleet_monitoring.vehicle_telemetry
WHERE timestamp > maxTimeuuid('2025-07-19 16:20:00');

-- Panel 2: Average Speed by Vehicle (Bar Chart)
SELECT vehicle_id, AVG(speed) as avg_speed
FROM fleet_monitoring.vehicle_telemetry
WHERE timestamp > maxTimeuuid('2025-07-19 16:00:00')
GROUP BY vehicle_id;

-- Panel 3: Speed vs Temperature (Scatter Plot)
SELECT speed, engine_temp, vehicle_id
FROM fleet_monitoring.vehicle_telemetry
WHERE timestamp > maxTimeuuid('2025-07-19 16:00:00');

-- Panel 4: Fuel Levels (Gauge)
SELECT vehicle_id, fuel_level
FROM fleet_monitoring.vehicle_telemetry
WHERE timestamp > maxTimeuuid('2025-07-19 16:25:00')
ORDER BY timestamp DESC
LIMIT 10;

-- ===============================================
-- ALERT QUERIES
-- ===============================================

-- Critical Temperature Alert
SELECT vehicle_id, engine_temp, timestamp
FROM fleet_monitoring.vehicle_telemetry
WHERE vehicle_id IN ('TRUCK_001', 'TRUCK_002', 'TRUCK_003', 'VAN_001', 'VAN_002')
AND timestamp > maxTimeuuid('2025-07-19 16:00:00')
AND engine_temp > 100
ORDER BY timestamp DESC;

-- Low Fuel Alert
SELECT vehicle_id, fuel_level, timestamp
FROM fleet_monitoring.vehicle_telemetry
WHERE vehicle_id IN ('TRUCK_001', 'TRUCK_002', 'TRUCK_003', 'VAN_001', 'VAN_002')
AND timestamp > maxTimeuuid('2025-07-19 16:00:00')
AND fuel_level < 20
ORDER BY timestamp DESC;

-- High Speed Alert
SELECT vehicle_id, speed, timestamp
FROM fleet_monitoring.vehicle_telemetry
WHERE vehicle_id IN ('TRUCK_001', 'TRUCK_002', 'TRUCK_003', 'VAN_001', 'VAN_002')
AND timestamp > maxTimeuuid('2025-07-19 16:00:00')
AND speed > 100
ORDER BY timestamp DESC; 