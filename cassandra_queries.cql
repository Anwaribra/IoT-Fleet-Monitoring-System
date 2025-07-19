-- ===============================================
-- WORKING CASSANDRA QUERIES FOR GRAFANA
-- ===============================================
-- Table Structure: vehicle_id (PK), timestamp (clustering), speed, engine_temp, fuel_level, latitude, longitude

-- 1. CURRENT VEHICLE STATUS (Table Panel)
-- ===============================================
SELECT vehicle_id, speed, engine_temp, fuel_level, latitude, longitude, timestamp
FROM fleet_monitoring.vehicle_telemetry
WHERE vehicle_id = 'TRUCK_001'
LIMIT 10;

-- For all vehicles, use multiple queries:
-- Query 1: TRUCK_001
SELECT vehicle_id, speed, engine_temp, fuel_level, timestamp
FROM fleet_monitoring.vehicle_telemetry
WHERE vehicle_id = 'TRUCK_001'
LIMIT 5;

-- Query 2: TRUCK_002
SELECT vehicle_id, speed, engine_temp, fuel_level, timestamp
FROM fleet_monitoring.vehicle_telemetry
WHERE vehicle_id = 'TRUCK_002'
LIMIT 5;

-- Query 3: VAN_001
SELECT vehicle_id, speed, engine_temp, fuel_level, timestamp
FROM fleet_monitoring.vehicle_telemetry
WHERE vehicle_id = 'VAN_001'
LIMIT 5;

-- 2. TIME SERIES DATA (Line Charts)
-- ===============================================
-- Speed over time for specific vehicle
SELECT timestamp, speed
FROM fleet_monitoring.vehicle_telemetry
WHERE vehicle_id = 'TRUCK_001'
LIMIT 100;

-- Engine temperature over time
SELECT timestamp, engine_temp
FROM fleet_monitoring.vehicle_telemetry
WHERE vehicle_id = 'TRUCK_001'
LIMIT 100;

-- Fuel level over time
SELECT timestamp, fuel_level
FROM fleet_monitoring.vehicle_telemetry
WHERE vehicle_id = 'TRUCK_001'
LIMIT 100;

-- 3. GEOMAP DATA
-- ===============================================
-- Vehicle locations
SELECT latitude, longitude, vehicle_id, speed
FROM fleet_monitoring.vehicle_telemetry
WHERE vehicle_id = 'TRUCK_001'
LIMIT 20;

-- GPS track for vehicle
SELECT latitude, longitude, timestamp
FROM fleet_monitoring.vehicle_telemetry
WHERE vehicle_id = 'TRUCK_001'
LIMIT 50;

-- 4. SINGLE STAT PANELS
-- ===============================================
-- Latest speed for vehicle
SELECT speed
FROM fleet_monitoring.vehicle_telemetry
WHERE vehicle_id = 'TRUCK_001'
LIMIT 1;

-- Latest temperature
SELECT engine_temp
FROM fleet_monitoring.vehicle_telemetry
WHERE vehicle_id = 'TRUCK_001'
LIMIT 1;

-- Latest fuel level
SELECT fuel_level
FROM fleet_monitoring.vehicle_telemetry
WHERE vehicle_id = 'TRUCK_001'
LIMIT 1;

-- 5. ALERT CONDITIONS
-- ===============================================
-- High temperature records
SELECT vehicle_id, engine_temp, timestamp
FROM fleet_monitoring.vehicle_telemetry
WHERE vehicle_id = 'TRUCK_001'
LIMIT 20;
-- Use Transform in Grafana: Filter by engine_temp > 100

-- Low fuel records
SELECT vehicle_id, fuel_level, timestamp
FROM fleet_monitoring.vehicle_telemetry
WHERE vehicle_id = 'TRUCK_001'
LIMIT 20;
-- Use Transform in Grafana: Filter by fuel_level < 20

-- High speed records
SELECT vehicle_id, speed, timestamp
FROM fleet_monitoring.vehicle_telemetry
WHERE vehicle_id = 'TRUCK_001'
LIMIT 20;
-- Use Transform in Grafana: Filter by speed > 100

-- ===============================================
-- GRAFANA SETUP INSTRUCTIONS
-- ===============================================

/*
DATASOURCE CONFIGURATION:
1. Add Cassandra datasource plugin
2. Host: localhost:9042
3. Keyspace: fleet_monitoring
4. Username: cassandra (default)
5. Password: cassandra (default)

DASHBOARD VARIABLES:
Create a vehicle_id variable with values:
- TRUCK_001, TRUCK_002, TRUCK_003
- VAN_001, VAN_002
- BUS_001
- BIKE_001, BIKE_002
- CAR_001, CAR_002

PANEL CONFIGURATIONS:

📊 STAT PANEL - Current Speed:
Query: SELECT speed FROM fleet_monitoring.vehicle_telemetry WHERE vehicle_id = '$vehicle_id' LIMIT 1;
Display: Last value

📈 TIME SERIES - Speed Trend:
Query: SELECT timestamp, speed FROM fleet_monitoring.vehicle_telemetry WHERE vehicle_id = '$vehicle_id' LIMIT 100;
X-axis: timestamp
Y-axis: speed

📋 TABLE - Vehicle Status:
Query: SELECT vehicle_id, speed, engine_temp, fuel_level, timestamp FROM fleet_monitoring.vehicle_telemetry WHERE vehicle_id = '$vehicle_id' LIMIT 10;

🗺️ GEOMAP - Vehicle Location:
Query: SELECT latitude, longitude, vehicle_id FROM fleet_monitoring.vehicle_telemetry WHERE vehicle_id = '$vehicle_id' LIMIT 1;
Latitude: latitude
Longitude: longitude

🔥 GAUGE - Fuel Level:
Query: SELECT fuel_level FROM fleet_monitoring.vehicle_telemetry WHERE vehicle_id = '$vehicle_id' LIMIT 1;
Min: 0, Max: 100

TRANSFORMATIONS TO USE:
- Reduce: Last (for latest values)
- Filter by value: engine_temp > 100, fuel_level < 20, speed > 100
- Group by: vehicle_id (when combining multiple queries)
*/

-- ===============================================
-- MULTI-VEHICLE QUERIES FOR COMPARISON
-- ===============================================

-- All vehicles latest data (use in multiple queries)
-- Query A:
SELECT vehicle_id, speed, engine_temp, fuel_level, timestamp
FROM fleet_monitoring.vehicle_telemetry
WHERE vehicle_id IN ('TRUCK_001', 'TRUCK_002', 'TRUCK_003')
LIMIT 10;

-- Query B:
SELECT vehicle_id, speed, engine_temp, fuel_level, timestamp
FROM fleet_monitoring.vehicle_telemetry
WHERE vehicle_id IN ('VAN_001', 'VAN_002', 'BUS_001')
LIMIT 10;

-- Query C:
SELECT vehicle_id, speed, engine_temp, fuel_level, timestamp
FROM fleet_monitoring.vehicle_telemetry
WHERE vehicle_id IN ('BIKE_001', 'BIKE_002', 'CAR_001', 'CAR_002')
LIMIT 10;

-- ===============================================
-- SAMPLE WORKING QUERIES TO TEST
-- ===============================================

-- Test Query 1: Get latest data for TRUCK_001
SELECT vehicle_id, speed, engine_temp, fuel_level, timestamp
FROM fleet_monitoring.vehicle_telemetry
WHERE vehicle_id = 'TRUCK_001'
LIMIT 5;

-- Test Query 2: Get speed trend for TRUCK_001
SELECT timestamp, speed
FROM fleet_monitoring.vehicle_telemetry
WHERE vehicle_id = 'TRUCK_001'
LIMIT 50;

-- Test Query 3: Get GPS locations for TRUCK_001
SELECT latitude, longitude, timestamp
FROM fleet_monitoring.vehicle_telemetry
WHERE vehicle_id = 'TRUCK_001'
LIMIT 20; 