-- GRAFANA VISUALIZATION QUERIES FOR IOT FLEET

-- 1. REAL-TIME FLEET DASHBOARD
-- ===============================================

-- Vehicle Count by Type (Pie Chart)
SELECT vehicle_type, COUNT(DISTINCT vehicle_id) as vehicle_count
FROM fleet_monitoring.vehicle_telemetry 
WHERE timestamp > NOW() - INTERVAL '1 hour'
GROUP BY vehicle_type;

-- Live Vehicle Status (Table)
SELECT 
    vehicle_id,
    vehicle_type,
    speed,
    engine_temp,
    fuel_level,
    timestamp
FROM fleet_monitoring.vehicle_telemetry
WHERE timestamp > NOW() - INTERVAL '5 minutes'
ORDER BY timestamp DESC
LIMIT 50;

-- 2. SPEED ANALYTICS
-- ===============================================

-- Average Speed by Vehicle Type (Bar Chart)
SELECT 
    vehicle_type,
    AVG(speed) as avg_speed,
    MAX(speed) as max_speed,
    MIN(speed) as min_speed
FROM fleet_monitoring.vehicle_telemetry
WHERE timestamp > NOW() - INTERVAL '1 hour'
GROUP BY vehicle_type;

-- Speed Over Time (Time Series)
SELECT 
    timestamp,
    vehicle_id,
    speed
FROM fleet_monitoring.vehicle_telemetry
WHERE timestamp > $__timeFrom AND timestamp < $__timeTo
ORDER BY timestamp;

-- Speed Distribution (Histogram)
SELECT 
    CASE 
        WHEN speed < 30 THEN '0-30 km/h'
        WHEN speed < 60 THEN '30-60 km/h'
        WHEN speed < 90 THEN '60-90 km/h'
        ELSE '90+ km/h'
    END as speed_range,
    COUNT(*) as count
FROM fleet_monitoring.vehicle_telemetry
WHERE timestamp > NOW() - INTERVAL '1 hour'
GROUP BY speed_range;

-- 3. ENGINE TEMPERATURE MONITORING
-- ===============================================

-- Engine Temperature Heatmap
SELECT 
    vehicle_id,
    AVG(engine_temp) as avg_temp,
    MAX(engine_temp) as max_temp
FROM fleet_monitoring.vehicle_telemetry
WHERE timestamp > NOW() - INTERVAL '1 hour'
GROUP BY vehicle_id;

-- Temperature Alerts (Stat Panel)
SELECT COUNT(*) as high_temp_alerts
FROM fleet_monitoring.vehicle_telemetry
WHERE engine_temp > 100 
AND timestamp > NOW() - INTERVAL '1 hour';

-- Temperature Trend (Time Series)
SELECT 
    timestamp,
    vehicle_id,
    engine_temp
FROM fleet_monitoring.vehicle_telemetry
WHERE timestamp > $__timeFrom AND timestamp < $__timeTo
AND vehicle_id IN ('TRUCK_001', 'TRUCK_002', 'VAN_001')
ORDER BY timestamp;

-- 4. FUEL MONITORING
-- ===============================================

-- Fuel Level by Vehicle (Gauge)
SELECT 
    vehicle_id,
    fuel_level,
    timestamp
FROM fleet_monitoring.vehicle_telemetry
WHERE timestamp > NOW() - INTERVAL '5 minutes';

-- Low Fuel Alerts (Stat Panel)
SELECT COUNT(DISTINCT vehicle_id) as low_fuel_vehicles
FROM fleet_monitoring.vehicle_telemetry
WHERE fuel_level < 20
AND timestamp > NOW() - INTERVAL '1 hour';

-- Fuel Consumption Rate (Time Series)
SELECT 
    timestamp,
    vehicle_id,
    fuel_level
FROM fleet_monitoring.vehicle_telemetry
WHERE timestamp > $__timeFrom AND timestamp < $__timeTo
ORDER BY timestamp;

-- 5. FLEET PERFORMANCE METRICS
-- ===============================================

-- Fleet Efficiency Score (Stat Panel)
SELECT 
    ROUND(AVG(
        CASE 
            WHEN speed BETWEEN 40 AND 80 AND engine_temp < 95 AND fuel_level > 20 
            THEN 100 
            ELSE 70 
        END
    ), 2) as efficiency_score
FROM fleet_monitoring.vehicle_telemetry
WHERE timestamp > NOW() - INTERVAL '1 hour';

-- Active Vehicles (Stat Panel)
SELECT COUNT(DISTINCT vehicle_id) as active_vehicles
FROM fleet_monitoring.vehicle_telemetry
WHERE timestamp > NOW() - INTERVAL '10 minutes';

-- Total Distance Covered (Stat Panel)
SELECT SUM(speed * 0.016667) as total_km_last_hour
FROM fleet_monitoring.vehicle_telemetry
WHERE timestamp > NOW() - INTERVAL '1 hour';

-- 6. GEOGRAPHIC VISUALIZATION
-- ===============================================

-- Vehicle Locations (Geomap)
SELECT 
    vehicle_id,
    latitude,
    longitude,
    speed,
    timestamp
FROM fleet_monitoring.vehicle_telemetry
WHERE timestamp > NOW() - INTERVAL '5 minutes';

-- GPS Track for Specific Vehicle (Geomap)
SELECT 
    latitude,
    longitude,
    speed,
    timestamp
FROM fleet_monitoring.vehicle_telemetry
WHERE vehicle_id = '$vehicle_id'
AND timestamp > $__timeFrom AND timestamp < $__timeTo
ORDER BY timestamp;

-- 7. ALERT DASHBOARD
-- ===============================================

-- Critical Alerts Summary (Table)
SELECT 
    vehicle_id,
    CASE 
        WHEN engine_temp > 110 THEN 'CRITICAL: High Temperature'
        WHEN fuel_level < 10 THEN 'CRITICAL: Low Fuel'
        WHEN speed > 120 THEN 'WARNING: Excessive Speed'
        ELSE 'OK'
    END as alert_type,
    engine_temp,
    fuel_level,
    speed,
    timestamp
FROM fleet_monitoring.vehicle_telemetry
WHERE (engine_temp > 100 OR fuel_level < 20 OR speed > 100)
AND timestamp > NOW() - INTERVAL '1 hour'
ORDER BY timestamp DESC;

-- Alert Count by Type (Pie Chart)
SELECT 
    CASE 
        WHEN engine_temp > 110 THEN 'High Temperature'
        WHEN fuel_level < 10 THEN 'Low Fuel'
        WHEN speed > 120 THEN 'Excessive Speed'
    END as alert_type,
    COUNT(*) as alert_count
FROM fleet_monitoring.vehicle_telemetry
WHERE (engine_temp > 110 OR fuel_level < 10 OR speed > 120)
AND timestamp > NOW() - INTERVAL '1 hour'
GROUP BY alert_type;

-- 8. HOURLY ANALYTICS
-- ===============================================

-- Hourly Fleet Summary (Time Series)
SELECT 
    DATE_TRUNC('hour', timestamp) as hour,
    COUNT(*) as total_records,
    AVG(speed) as avg_speed,
    AVG(engine_temp) as avg_temp,
    AVG(fuel_level) as avg_fuel
FROM fleet_monitoring.vehicle_telemetry
WHERE timestamp > NOW() - INTERVAL '24 hours'
GROUP BY hour
ORDER BY hour;

-- Peak Activity Hours (Bar Chart)
SELECT 
    EXTRACT(HOUR FROM timestamp) as hour_of_day,
    COUNT(*) as activity_count,
    AVG(speed) as avg_speed
FROM fleet_monitoring.vehicle_telemetry
WHERE timestamp > NOW() - INTERVAL '24 hours'
GROUP BY hour_of_day
ORDER BY hour_of_day;

-- ===============================================
-- GRAFANA DASHBOARD RECOMMENDATIONS
-- 
/*
DASHBOARD 1: REAL-TIME FLEET OVERVIEW
- Stat Panels: Active Vehicles, Total Distance, Efficiency Score
- Pie Chart: Vehicle Types
- Table: Live Vehicle Status
- Geomap: Current Vehicle Locations

DASHBOARD 2: PERFORMANCE ANALYTICS  
- Time Series: Speed & Temperature Trends
- Bar Charts: Speed by Vehicle Type
- Heatmap: Engine Temperature
- Gauges: Fuel Levels

DASHBOARD 3: ALERTS & MONITORING
- Stat Panels: Alert Counts
- Table: Critical Alerts
- Time Series: Alert Trends
- Pie Chart: Alert Distribution

DASHBOARD 4: HISTORICAL ANALYSIS
- Time Series: Hourly Trends
- Bar Chart: Peak Activity Hours
- Histogram: Speed Distribution
- Line Chart: Fuel Consumption
*/ 