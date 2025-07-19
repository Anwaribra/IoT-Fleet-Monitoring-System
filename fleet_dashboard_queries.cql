FLEET MONITORING DASHBOARD QUERIES
-- ============================

-- 1. FLEET OVERVIEW STATS
-- =====================================

-- Active Vehicles Status
SELECT vehicle_id, speed, engine_temp, fuel_level, timestamp
FROM fleet_monitoring.vehicle_telemetry
WHERE vehicle_id IN ('TRUCK_001', 'TRUCK_002', 'TRUCK_003', 'VAN_001', 'VAN_002', 'BUS_001', 'BIKE_001', 'BIKE_002', 'CAR_001', 'CAR_002')
LIMIT 20;
-- Use Transform: Group by vehicle_id, Reduce: Last

-- High Temperature Vehicles
SELECT vehicle_id, engine_temp, timestamp
FROM fleet_monitoring.vehicle_telemetry
WHERE vehicle_id IN ('TRUCK_001', 'TRUCK_002', 'TRUCK_003', 'VAN_001', 'VAN_002')
AND engine_temp > 100
LIMIT 10;

-- Low Fuel Alerts
SELECT vehicle_id, fuel_level, timestamp
FROM fleet_monitoring.vehicle_telemetry
WHERE vehicle_id IN ('TRUCK_001', 'TRUCK_002', 'TRUCK_003', 'VAN_001', 'VAN_002')
AND fuel_level < 20
LIMIT 10;

-- 2. SPEED MONITORING
-- ================================

-- Speed Comparison (Time Series)
SELECT timestamp, speed, vehicle_id
FROM fleet_monitoring.vehicle_telemetry
WHERE vehicle_id IN ('TRUCK_001', 'VAN_001', 'BIKE_001', 'CAR_001')
LIMIT 100;

-- High Speed Alerts
SELECT vehicle_id, speed, timestamp
FROM fleet_monitoring.vehicle_telemetry
WHERE vehicle_id IN ('TRUCK_001', 'TRUCK_002', 'VAN_001', 'BIKE_001')
LIMIT 20;
-- Use Transform: Filter speed > 100

-- 3. TEMPERATURE ANALYSIS
-- ================================

-- Engine Temperature Trends
SELECT timestamp, engine_temp, vehicle_id
FROM fleet_monitoring.vehicle_telemetry
WHERE vehicle_id IN ('TRUCK_001', 'TRUCK_002', 'VAN_001')
LIMIT 100;

-- Critical Temperature Monitoring
SELECT vehicle_id, engine_temp, timestamp
FROM fleet_monitoring.vehicle_telemetry
WHERE vehicle_id IN ('TRUCK_001', 'TRUCK_002', 'TRUCK_003')
LIMIT 30;
-- Use Transform: Filter engine_temp > 110

-- 4. FUEL MONITORING
-- ========================

-- Fuel Level Status
SELECT vehicle_id, fuel_level, timestamp
FROM fleet_monitoring.vehicle_telemetry
WHERE vehicle_id IN ('TRUCK_001', 'TRUCK_002', 'VAN_001', 'BUS_001')
LIMIT 20;
-- Use Transform: Group by vehicle_id, Reduce: Last

-- Fuel Consumption Trend
SELECT timestamp, fuel_level, vehicle_id
FROM fleet_monitoring.vehicle_telemetry
WHERE vehicle_id IN ('TRUCK_001', 'VAN_001')
LIMIT 100;

-- 5. GPS TRACKING
-- ============================

-- Current Vehicle Locations
SELECT vehicle_id, latitude, longitude, speed
FROM fleet_monitoring.vehicle_telemetry
WHERE vehicle_id IN ('TRUCK_001', 'TRUCK_002', 'VAN_001', 'BIKE_001', 'CAR_001')
LIMIT 10;
-- Use Transform: Group by vehicle_id, Reduce: Last

-- Vehicle Route Tracking
SELECT timestamp, latitude, longitude, speed
FROM fleet_monitoring.vehicle_telemetry
WHERE vehicle_id = '$vehicle_id'
LIMIT 50;

-- 6. PERFORMANCE METRICS
-- ====================================

-- Vehicle Speed Stats
SELECT vehicle_id, speed
FROM fleet_monitoring.vehicle_telemetry
WHERE vehicle_id IN ('TRUCK_001', 'TRUCK_002', 'VAN_001', 'BIKE_001')
LIMIT 100;
-- Use Transform: Group by vehicle_id, Calculate: Avg, Max, Min

-- Engine Performance
SELECT vehicle_id, engine_temp, speed
FROM fleet_monitoring.vehicle_telemetry
WHERE vehicle_id IN ('TRUCK_001', 'TRUCK_002', 'VAN_001')
LIMIT 50;
-- Use Transform: Calculate correlation between speed and temperature 